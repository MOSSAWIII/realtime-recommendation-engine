package core

import models._
import repositories._
import config.{DatabaseConfig, RabbitMQManager}
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import scala.util.{Success, Failure, Try}
import org.slf4j.LoggerFactory

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  def main(args: Array[String]): Unit = {
    logger.info("Démarrage du moteur de recommandation en temps réel...")
    
    try {
      // 1. Initialisation des repositories
      logger.info("Initialisation des repositories...")
      val repository = new Repository()
      val userRepo = new UserRepository()
      val eventRepo = new EventRepository()
      val participationRepo = new ParticipationRepository()
      val friendsRepo = new FriendsRepository()
      
      // 2. Services métier (dépendent des repositories)
      logger.info("Initialisation des services...")
      val recommender = new Recommender(repository)
      val transformer = new Transformer(repository)
      
      // 3. Consumers (dépendent des services)
      logger.info("Initialisation des consumers...")
      val streamingConsumer = new StreamingConsumer(transformer)
      val consumerManager = new ConsumerManager(
        userRepo, eventRepo, participationRepo, friendsRepo
      )
      
      // 4. Data loader (dépend des repositories)
      logger.info("Initialisation du data loader...")
      val dataLoader = new DataLoader(
        userRepo, eventRepo, participationRepo, friendsRepo
      )
      
      // 5. Initialisation des tables
      logger.info("Initialisation des tables...")
      Await.result(repository.initTables(), 30.seconds)
      logger.info("Tables initialisées avec succès")
      
      // 6. Chargement des données initiales (sans cache)
      logger.info("Chargement des données initiales...")
      dataLoader.loadAll()
      logger.info("Données chargées avec succès")
      
      // 7. Démarrage des services
      logger.info("Démarrage des services...")
      recommender.start() // Démarre le consumer interne pour les demandes de recommandation
      
      // 8. Démarrage des consumers
      logger.info("Démarrage des consumers...")
      streamingConsumer.start()
      consumerManager.startAllConsumers()
      
      // 9. Tâche périodique pour les recommandations en batch
      val recommendationThread = new Thread(() => {
        while (true) {
          try {
            logger.info("=== Génération périodique des recommandations ===")
            
            Await.result(
              recommender.generateRecommendationsForAllUsers(), 
              60.seconds
            )
            
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
      
      // 10. Attente de l'arrêt (Ctrl+C)
      logger.info("Moteur de recommandation démarré avec succès")
      logger.info("Appuyez sur Ctrl+C pour arrêter le système...")
      Thread.currentThread().join()
      
    } catch {
      case ex: Exception =>
        logger.error(s"Erreur critique: ${ex.getMessage}", ex)
    } finally {
      // Nettoyage des ressources
      Try(RabbitMQManager.closeAllChannels())
      Try(RabbitMQManager.closeConnection())
      Try(DatabaseConfig.closePool())
      
      logger.info("Système arrêté")
    }
  }
}