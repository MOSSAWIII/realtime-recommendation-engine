package application

import domain.models._
import infrastructure.messaging.rabbitmq.RabbitMQManager
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import infrastructure.messaging.MessageSchemaManager
import utils.metrics.SystemStatus
import java.util.concurrent.TimeUnit
import infrastructure.persistence.RepositoryFactory

object Main {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    logger.info("Starting Recommendation Engine")
    
    try {
      // Initialize RabbitMQ message topology according to the technical architecture
      RabbitMQManager.setupExchangesAndQueues()
      
      // Register message schemas
      MessageSchemaManager.registerAllSchemas()
      
      // Start system status monitoring
      SystemStatus.startMonitoring()
      
      // Initialize components
      initComponents()
      
      // Start periodic database stats reporting
      val dbStatsScheduler = java.util.concurrent.Executors.newScheduledThreadPool(1)
      dbStatsScheduler.scheduleAtFixedRate(
        () => logger.info(dataManager.getDatabaseStats()),
        5,
        15,
        TimeUnit.MINUTES
      )
      
      // Register shutdown hook
      sys.addShutdownHook {
        logger.info("Shutting down application...")
        dbStatsScheduler.shutdown()
        closeAll(streamingConsumer, recommender, dataManager)
      }
      
      // Keep application running
      while (true) {
        Thread.sleep(60000)
        logger.info("Application running - components healthy")
      }
    } catch {
      case e: Exception =>
        logger.error("Fatal error during startup", e)
        System.exit(1)
    }
  }
  
  private def initComponents(): Unit = {
    // Create repositories with appropriate access levels
    val readWriteRepo = RepositoryFactory.createReadWrite()
    val readOnlyRepo = RepositoryFactory.createReadOnly()
    
    // Initialize components with appropriate repository access
    val transformer = new Transformer() // No repository dependency anymore
    val dataManager = new DataManager(readWriteRepo) // Full read-write access
    val recommender = new Recommender(readOnlyRepo) // Read-only access
    
    // Rest of component initialization...
    try {
      Await.result(dataManager.initTables(), 30.seconds)
      logger.info("Database tables initialized successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to initialize database tables: ${e.getMessage}", e)
        System.exit(1)
    }
    
    val streamingConsumer = new StreamingConsumer(transformer)
    
    try {
      recommender.start()
      streamingConsumer.start()
      logger.info("All components started successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to start components: ${e.getMessage}", e)
        closeAll(streamingConsumer, recommender, dataManager)
        System.exit(1)
    }
  }
  
  private def closeAll(streamingConsumer: StreamingConsumer, recommender: Recommender, dataManager: DataManager): Unit = {
    streamingConsumer.close()
    recommender.close()
    dataManager.close()
    RabbitMQManager.closeAllChannels()
    SystemStatus.stopMonitoring()
    logger.info("All components shut down")
  }
}