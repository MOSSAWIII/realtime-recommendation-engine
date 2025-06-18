package integration.db

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import application.core.DataManager
import domain.models.ProcessedEvent
import java.sql.DriverManager
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import common.mocks.{TestConfigurationService, TestDatabaseProvider}

class DataManagerIntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  var dataManager: DataManager = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    // Use environment variables set by docker-compose-test.yml
    val dbHost = sys.env.getOrElse("DATABASE_HOST", "test-db")
    val dbPort = sys.env.getOrElse("DATABASE_PORT", "5432")
    val dbName = sys.env.getOrElse("DATABASE_NAME", "test_recommendation_engine")
    val jdbcUrl = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
    val username = sys.env.getOrElse("DATABASE_USER", "postgres")
    val password = sys.env.getOrElse("DATABASE_PASSWORD", "test_password")
    
    // Print connection details for debugging
    println(s"Connecting to database: $jdbcUrl with user: $username")
    
    // Add logging for connection parameters
    val props = new java.util.Properties()
    props.setProperty("user", username)
    props.setProperty("password", password)
    props.setProperty("connectTimeout", "5")
    props.setProperty("socketTimeout", "10")
    
    try {
      println(s"Testing direct connection to: $jdbcUrl")
      val testConn = DriverManager.getConnection(jdbcUrl, props)
      println("Direct connection successful!")
      testConn.close()
    } catch {
      case e: Exception => 
        println(s"Direct connection failed: ${e.getMessage}")
        e.printStackTrace()
    }
    
    // Wait for database to be ready
    Thread.sleep(5000)
    
    // Initialize database schema first
    val connection = DriverManager.getConnection(jdbcUrl, username, password)
    val statement = connection.createStatement()
    statement.execute("""
      CREATE TABLE IF NOT EXISTS processed_events (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        event_type VARCHAR(100) NOT NULL,
        item_id BIGINT,
        properties TEXT,
        timestamp BIGINT NOT NULL,
        processing_timestamp BIGINT NOT NULL
      )
    """)
    statement.close()
    connection.close()
    
    // Create proper configuration services with container connection details
    val configService = new TestConfigurationService() {
      override def getDatabaseUrl(): String = jdbcUrl
      override def getDatabaseUser(): String = username
      override def getDatabasePassword(): String = password
    }
    
    val dbProvider = new TestDatabaseProvider() {
      override def getConnection(): java.sql.Connection = {
        DriverManager.getConnection(jdbcUrl, username, password)
      }
    }
    
    // Initialize DataManager with proper constructor parameters
    dataManager = new DataManager(configService, dbProvider)
    
    // Initialize tables
    Await.result(dataManager.initTables(), 60.seconds)
  }
  
  override def afterAll(): Unit = {
    if (dataManager != null) {
      dataManager.shutdown()
    }
    super.afterAll()
  }
  
  "DataManager" should "retrieve stored events for a user" in {
    // Create a test event
    val testEvent = ProcessedEvent(
      userId = 123L,
      eventType = "test_event",
      itemId = Some(456L),
      properties = Map("key" -> "value"),
      timestamp = System.currentTimeMillis(),
      processingTimestamp = System.currentTimeMillis()
    )
    
    // Store the event
    val storedEvent = Await.result(dataManager.storeProcessedEvent(testEvent), 60.seconds)
    storedEvent.id should not be None
    
    // Retrieve events for the user
    val retrievedEvents = Await.result(dataManager.getProcessedEventsForUser(123L), 60.seconds)
    
    retrievedEvents should not be empty
    retrievedEvents.head.userId shouldBe 123L
    retrievedEvents.head.eventType shouldBe "test_event"
    retrievedEvents.head.itemId shouldBe Some(456L)
  }
}

