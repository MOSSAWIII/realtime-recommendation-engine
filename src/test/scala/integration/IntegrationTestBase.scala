package integration

import com.dimafeng.testcontainers.{PostgreSQLContainer, ForAllTestContainer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import config.RabbitMQManager

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
  
  // Define both containers in a container
  override val container = {
    // Start PostgreSQL container
    val postgres = PostgreSQLContainer()
      .configure { container =>
        container.withDatabaseName("test_recommendation_engine")
        container.withUsername("postgres")
        container.withPassword("test_password")
      }
    
    // Use our wrapper for RabbitMQ container
    val rabbitMq = new RabbitMQContainer("rabbitmq:3-management")
    
    // Use MultipleContainers to combine both
    com.dimafeng.testcontainers.MultipleContainers(postgres, rabbitMq)
  }

  // Define container accessor methods to make them available to tests
  protected def postgresContainer: PostgreSQLContainer = 
    container.containers.head.asInstanceOf[PostgreSQLContainer]
    
  protected def rabbitMqContainer: RabbitMQContainer = 
    container.containers(1).asInstanceOf[RabbitMQContainer]
  
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

