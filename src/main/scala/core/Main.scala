package core

import models._
import repositories._
import config.{DatabaseConfig, RabbitMQManager}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import org.slf4j.LoggerFactory

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    logger.info("Starting Recommendation Engine")
    
    // Initialize components
    val repository = new ConcreteRepository()
    
    // Initialize database tables
    try {
      Await.result(repository.initTables(), 30.seconds)
      logger.info("Database tables initialized successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to initialize database tables: ${e.getMessage}", e)
        System.exit(1)
    }
    
    // Create our pipeline components
    val transformer = new Transformer(repository)
    val recommender = new Recommender(repository)
    val streamingConsumer = new StreamingConsumer(transformer)
    
    // Start reference data consumers
    val userRepo = new UserRepository()
    val eventRepo = new EventRepository()
    val participationRepo = new ParticipationRepository()
    val friendsRepo = new FriendsRepository()
    
    val consumerManager = new ConsumerManager(
      userRepo, eventRepo, participationRepo, friendsRepo
    )
    
    // Start components
    try {
      // Start recommender service first to handle recommendation requests
      recommender.start()
      logger.info("Recommender service started")
      
      // Start streaming consumer to process incoming user events
      streamingConsumer.start()
      logger.info("Streaming consumer started")
      
      // Start reference data consumers
      consumerManager.startAllConsumers()
      
      logger.info("Recommendation Engine running. Press CTRL+C to exit.")
      
      // Keep the application running
      while (true) {
        Thread.sleep(10000)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error starting recommendation engine: ${e.getMessage}", e)
        closeAll(streamingConsumer, recommender, consumerManager)
        System.exit(1)
    }
    
    sys.addShutdownHook {
      logger.info("Shutting down Recommendation Engine")
      closeAll(streamingConsumer, recommender, consumerManager)
    }
  }
  
  private def closeAll(
    streamingConsumer: StreamingConsumer, 
    recommender: Recommender,
    consumerManager: ConsumerManager
  ): Unit = {
    try {
      streamingConsumer.close()
      recommender.close()
      consumerManager.close()
      RabbitMQManager.closeAllChannels()
      RabbitMQManager.closeConnection()
    } catch {
      case e: Exception =>
        logger.error(s"Error during shutdown: ${e.getMessage}", e)
    }
  }
}