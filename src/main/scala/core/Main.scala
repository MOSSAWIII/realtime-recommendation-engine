package core

import models._
import repositories.Repository
import config.{DatabaseConfig, RabbitMQConfig}
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import scala.util.{Success, Failure, Try}
import org.slf4j.LoggerFactory

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  def main(args: Array[String]): Unit = {
    logger.info("Démarrage du moteur de recommandation en temps réel...")
    
    // Initialisation des composants
    val repository = new Repository()
    val recommender = new Recommender(repository)
    val transformer = new Transformer(repository, recommender)
    val streamingConsumer = new StreamingConsumer(transformer)
    
    try {
      // 1. Initialisation des tables
      logger.info("Initialisation des tables...")
      Await.result(repository.initTables(), 30.seconds)
      logger.info("Tables initialisées avec succès")
      
      // 2. Démarrage du consumer d'événements
      streamingConsumer.start()
      
      // 3. Génération périodique des recommandations pour tous les utilisateurs
      val recommendationThread = new Thread(() => {
        while (true) {
          try {
            logger.info("=== Génération périodique des recommandations ===")
            
            Await.result(
              recommender.generateRecommendationsForAllUsers(), 
              60.seconds
            )
            
            // Pause entre les générations de recommandations
            Thread.sleep(5 * 60 * 1000) // 5 minutes
          } catch {
            case ex: Exception =>
              logger.error(s"Erreur dans la tâche périodique: ${ex.getMessage}", ex)
              Thread.sleep(30 * 1000) // Attendre 30 secondes avant de réessayer
          }
        }
      })
      
      recommendationThread.setDaemon(true)
      recommendationThread.start()
      
      // 4. Attente de l'arrêt (Ctrl+C)
      logger.info("Moteur de recommandation démarré avec succès")
      logger.info("Appuyez sur Ctrl+C pour arrêter le système...")
      Thread.currentThread().join()
      
    } catch {
      case ex: Exception =>
        logger.error(s"Erreur critique: ${ex.getMessage}", ex)
    } finally {
      // Nettoyage des ressources
      Try(streamingConsumer.close())
      Try(recommender.close())
      Try(RabbitMQConfig.closeConnection())
      Try(DatabaseConfig.closePool())
      logger.info("Système arrêté")
    }
  }
}