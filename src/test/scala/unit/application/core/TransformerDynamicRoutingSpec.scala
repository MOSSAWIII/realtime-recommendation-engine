package unit.application.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import application.core.{Transformer, DataManager}
import domain.models.{UserEvent, ProcessedEvent}
import common.fixtures.TestDataFactory
import common.mocks.{MockDataManager, TestMockManager}
import common.mocks.{TestConfigurationService, TestDatabaseProvider}
import common.base.RabbitMQTestBase

class TransformerDynamicRoutingSpec extends RabbitMQTestBase {
  
  implicit val ec: ExecutionContext = ExecutionContext.global

  "Transformer dynamic routing" should "use userId in routing keys for recommendations" in {
    val configService = new TestConfigurationService()
    val dbProvider = new TestDatabaseProvider()
    val mockDataManager = new MockDataManager(configService, dbProvider)
    val transformer = new Transformer(mockDataManager)
    val testUserId = 12345L

    try {
      val testEvent = TestDataFactory.createUserEvent(
        userId = testUserId,
        eventType = "view",
        itemId = Some(67890L)
      )

      val result = Await.result(transformer.processEvent(testEvent), 5.seconds)

      val expectedRoutingKey = s"recommendation.request.${testUserId}"
      
      Thread.sleep(1000)
      val message = consumeTestMessage("recommendations_queue")
      
      message should not be empty
      message.get should include(testUserId.toString)

    } finally {
      transformer.close()
    }
  }

  "Transformer" should "publish events with dynamic routing keys based on user characteristics" in {
    val configService = TestMockManager.createMockConfigService()
    val dbProvider = TestMockManager.createMockDatabaseProvider()
    val mockDataManager = new MockDataManager(configService, dbProvider)(ec)
    val transformer = new Transformer(mockDataManager)(ec)
    val testUserId = 12345L

    try {
      val testEvent = TestDataFactory.createUserEvent(
        userId = testUserId,
        eventType = "view",
        itemId = Some(67890L)
      )

      val result = Await.result(transformer.processEvent(testEvent), 5.seconds)

      Thread.sleep(1000)
      val processedMessage = consumeTestMessage("processed_events_queue")
      
      processedMessage should not be empty
      processedMessage.get should include("view")
      processedMessage.get should include(testUserId.toString)

    } finally {
      transformer.close()
    }
  }

  "Transformer" should "publish processed events with correct routing patterns" in {
    val configService = TestMockManager.createMockConfigService()
    val dbProvider = TestMockManager.createMockDatabaseProvider()
    val mockDataManager = new MockDataManager(configService, dbProvider)(ec)
    val transformer = new Transformer(mockDataManager)(ec)
    val testUserId = 98765L

    try {
      val testEvent = TestDataFactory.createUserEvent(
        userId = testUserId,
        eventType = "purchase"
      )

      val result = Await.result(transformer.processEvent(testEvent), 5.seconds)

      Thread.sleep(1000)
      val processedMessage = consumeTestMessage("processed_events_queue")
      
      processedMessage should not be empty
      processedMessage.get should include("purchase")
      processedMessage.get should include(testUserId.toString)

    } finally {
      transformer.close()
    }
  }

  "Transformer" should "handle user segmentation for premium users" in {
    val configService = TestMockManager.createMockConfigService()
    val dbProvider = TestMockManager.createMockDatabaseProvider()
    val mockDataManager = new MockDataManager(configService, dbProvider)(ec)
    val transformer = new Transformer(mockDataManager)(ec)
    val testUserId = 54321L

    try {
      val testEvent = TestDataFactory.createUserEvent(
        userId = testUserId,
        eventType = "premium_access"
      )

      val result = Await.result(transformer.processEvent(testEvent), 5.seconds)

      Thread.sleep(1000)
      val premiumMessage = consumeTestMessage("premium_users_queue")
      
      premiumMessage should not be empty
      premiumMessage.get should include("premium_access")
      premiumMessage.get should include(testUserId.toString)

    } finally {
      transformer.close()
    }
  }
}

