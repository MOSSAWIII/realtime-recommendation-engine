package core

import models._
import repositories.Repository
import config.{DatabaseConfig, RabbitMQConfig}
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import scala.util.{Success, Failure}

object Main {
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  def main(args: Array[String]): Unit = {
    println("Démarrage du moteur de recommandation en temps réel...")
    
    // Initialisation des composants
    val repository = new Repository()
    val recommender = new Recommender(repository)
    val transformer = new Transformer(repository, recommender)
    val streamingConsumer = new StreamingConsumer(transformer)
    
    try {
      // 1. Initialisation des tables
      println("Initialisation des tables...")
      Await.result(repository.initTables(), 30.seconds)
      println("Tables initialisées avec succès")
      
      // 2. Démarrage du consumer d'événements
      streamingConsumer.start()
      
      // 3. Génération périodique des recommandations pour tous les utilisateurs
      val recommendationThread = new Thread(() => {
        while (true) {
          try {
            println("\n=== Génération périodique des recommandations ===")
            
            Await.result(
              recommender.generateRecommendationsForAllUsers(), 
              60.seconds
            )
            
            // Pause entre les générations de recommandations
            Thread.sleep(5 * 60 * 1000) // 5 minutes
          } catch {
            case ex: Exception =>
              println(s"Erreur dans la tâche périodique: ${ex.getMessage}")
              ex.printStackTrace()
              Thread.sleep(30 * 1000) // Attendre 30 secondes avant de réessayer
          }
        }
      })
      
      recommendationThread.setDaemon(true)
      recommendationThread.start()
      
      // 4. Attente de l'arrêt (Ctrl+C)
      println("\nMoteur de recommandation démarré avec succès")
      println("Appuyez sur Ctrl+C pour arrêter le système...")
      Thread.currentThread().join()
      
    } catch {
      case ex: Exception =>
        println(s"Erreur critique: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      // Nettoyage des ressources
      streamingConsumer.close()
      recommender.close()
      RabbitMQConfig.closeConnection()
      println("Système arrêté")
    }
  }
}