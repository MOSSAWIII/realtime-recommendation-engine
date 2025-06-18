package common.mocks

import scala.concurrent.ExecutionContext
import java.util.concurrent.CountDownLatch
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import com.rabbitmq.client.{Channel, Connection}
import java.sql.{Connection => SqlConnection}

/**
 * Factory centralisée pour la création de tous les mocks
 */
object RabbitMQTestManager {
  def createChannel(name: String): Channel = {
    mock(classOf[Channel])
  }
}

object MockFactory {
  // Ajouter ExecutionContext implicite global
  implicit val testExecutionContext: ExecutionContext = ExecutionContext.global
  
  // Configuration
  def createConfigService(): TestConfigurationService = new TestConfigurationService()
  
  // Base de données
  def createDatabaseProvider(): TestDatabaseProvider = new TestDatabaseProvider()
  
  def mockDatabaseConnection(): SqlConnection = {
    mock(classOf[SqlConnection])
  }
  
  // DataManager
  def createDataManager()(implicit ec: ExecutionContext = ExecutionContext.global): MockDataManager = {
    val configService = new TestConfigurationService()
    val dbProvider = new TestDatabaseProvider()
    
    // ASSURER que l'objet est complètement initialisé
    val mockDataManager = new MockDataManager(configService, dbProvider)(ec)
    
    // Initialisation de base pour éviter les null
    mockDataManager.initializeMockData()
    
    mockDataManager
  }
  
  // RabbitMQ
  def createRabbitMQChannel(): com.rabbitmq.client.Channel = {
    RabbitMQTestManager.createChannel(s"test-channel-${System.currentTimeMillis}")
  }
  
  def mockRabbitMQChannel(): Channel = {
    RabbitMQTestManager.createChannel(s"test-channel-${System.currentTimeMillis}")
  }
}

