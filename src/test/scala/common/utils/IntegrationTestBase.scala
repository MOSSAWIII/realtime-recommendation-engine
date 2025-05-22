package integration

import com.dimafeng.testcontainers.{PostgreSQLContainer, ForAllTestContainer, MultipleContainers}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import common.utils.RabbitMQManager

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import java.util.UUID

abstract class IntegrationTestBase extends AnyWordSpec 
  with ForAllTestContainer 
  with BeforeAndAfterAll
  with Eventually
  with ScalaFutures
  with Matchers {

  // Configuration for the patience config
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(10, Seconds)),
    interval = scaled(Span(100, Millis))
  )
  
  // Define containers individually
  protected val pgContainer = PostgreSQLContainer()
    .configure { container =>
      container.withDatabaseName("test_recommendation_engine")
      container.withUsername("postgres")
      container.withPassword("test_password")
    }
  
  protected val rmqContainer = new RabbitMQContainer("rabbitmq:3-management")
  
  // Combined container for ForAllTestContainer trait
  override val container = MultipleContainers(pgContainer, rmqContainer)

  // Simplified accessor methods
  protected def postgresContainer: PostgreSQLContainer = pgContainer
  protected def rabbitMqContainer: RabbitMQContainer = rmqContainer
  
  // Test-specific configuration
  protected var testConfig: Config = _
  
  // Generate unique test ID for isolation
  protected val testId = UUID.randomUUID().toString
  
  // Implicit execution context for async operations
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  // Setup any resources needed by tests
  def setupTestResources(): Unit = {}
  
  // Clean up any resources used by tests
  def cleanupTestResources(): Unit = {}
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Create configuration based on container info
    testConfig = ConfigFactory.load("application-test.conf")
      .withValue("database.url", 
                 ConfigValueFactory.fromAnyRef(s"jdbc:postgresql://${postgresContainer.host}:${postgresContainer.mappedPort(5432)}/test_recommendation_engine"))
      .withValue("database.user", ConfigValueFactory.fromAnyRef(postgresContainer.username))
      .withValue("database.password", ConfigValueFactory.fromAnyRef(postgresContainer.password))
      .withValue("rabbitmq.host", ConfigValueFactory.fromAnyRef(rabbitMqContainer.getHost))
      .withValue("rabbitmq.port", ConfigValueFactory.fromAnyRef(rabbitMqContainer.getMappedPort(5672)))
    
    // Set up test resources
    setupTestResources()
  }
  
  override def afterAll(): Unit = {
    cleanupTestResources()
    RabbitMQManager.closeAllChannels()
    super.afterAll()
  }
}

