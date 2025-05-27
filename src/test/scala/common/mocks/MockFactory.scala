package common.mocks

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import com.rabbitmq.client.{Channel, Connection}
import java.sql.{Connection => SqlConnection}
import scala.concurrent.ExecutionContext

/**
 * Factory centralisée pour la création de tous les mocks
 */
object RabbitMQTestManager {
  def createChannel(name: String): Channel = {
    mock(classOf[Channel])
  }
}

object MockFactory {
  // Contexte d'exécution partagé pour tous les tests
  implicit val testExecutionContext: ExecutionContext = ExecutionContext.global
  
  // Configuration
  def createConfigService(): TestConfigurationService = new TestConfigurationService()
  
  // Base de données
  def createDatabaseProvider(): TestDatabaseProvider = new TestDatabaseProvider()
  
  def mockDatabaseConnection(): SqlConnection = {
    mock(classOf[SqlConnection])
  }
  
  // DataManager
  def createDataManager(withLatch: Boolean = false): MockDataManager = {
    val configService = createConfigService()
    val dbProvider = createDatabaseProvider()
    val latch = if (withLatch) new java.util.concurrent.CountDownLatch(1) else null
    new MockDataManager(configService, dbProvider, latch)(testExecutionContext)
  }
  
  // RabbitMQ
  def createRabbitMQChannel(): com.rabbitmq.client.Channel = {
    RabbitMQTestManager.createChannel(s"test-channel-${System.currentTimeMillis}")
  }
  
  def mockRabbitMQChannel(): Channel = {
    RabbitMQTestManager.createChannel(s"test-channel-${System.currentTimeMillis}")
  }
}

