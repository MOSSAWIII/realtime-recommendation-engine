package common.base

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import com.rabbitmq.client.Channel
import common.mocks.{MockFactory, TestConfigurationService, TestDatabaseProvider, MockDataManager}
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
import scala.concurrent.ExecutionContext

abstract class MessagingTestBase extends IntegrationTestBase {
  
  // Utiliser var au lieu de def pour permettre la réassignation
  protected var recommendationChannel: Channel = _
  private val resourcesToCleanup = scala.collection.mutable.ListBuffer[AutoCloseable]()
  
  protected val testExchange = sys.env.getOrElse("TEST_EXCHANGE", "user_actions_exchange")
  protected val testQueueName = sys.env.getOrElse("TEST_PROCESSED_EVENTS_QUEUE", "processed_events_queue")
  protected val testId = sys.env.getOrElse("TEST_RUN_ID", "default_test_id")
  
  protected def createTestChannel(name: String): Channel = {
    common.mocks.MockFactory.createRabbitMQChannel()
  }
  
  protected def createIsolatedQueue(channel: Channel, baseName: String): String = {
    val queueName = s"${baseName}_${System.currentTimeMillis()}"
    channel.queueDeclare(queueName, false, false, true, null)
    queueName
  }
  
  protected def registerForCleanup(resource: AutoCloseable): Unit = {
    resourcesToCleanup += resource
  }
  
  protected def createIsolatedDataManager(): MockDataManager = {
    new MockDataManager(
      new TestConfigurationService(),
      new TestDatabaseProvider(),
      null
    )(ExecutionContext.global)
  }
  
  override def afterEach(): Unit = {
    resourcesToCleanup.foreach { resource =>
      try {
        resource.close()
      } catch {
        case e: Exception => // Ignore cleanup errors in tests
      }
    }
    resourcesToCleanup.clear()
    super.afterEach()
  }
}