package common.base

import infrastructure.messaging.rabbitmq.RabbitMQManager
import infrastructure.config.ConfigurationService
import com.rabbitmq.client.{Channel, Connection}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import scala.util.Try

trait RabbitMQTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  
  protected val testVhost: String = sys.env.getOrElse("TEST_VHOST", "test_env")
  private var testChannel: Option[Channel] = None
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    setupTestRabbitMQ()
  }

  override def afterAll(): Unit = {
    cleanupTestRabbitMQ()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    purgeTestQueues()
  }

  private def setupTestRabbitMQ(): Unit = {
    try {
      testChannel = Some(RabbitMQManager.createChannel("test-setup"))
      validateTestEnvironment()
    } catch {
      case e: Exception =>
        fail(s"Failed to setup test RabbitMQ environment: ${e.getMessage}")
    }
  }

  private def validateTestEnvironment(): Unit = {
    testChannel.foreach { channel =>
      val requiredExchanges = List("user_actions_exchange", "dlx")
      val requiredQueues = List(
        "processed_events_queue", 
        "recommendations_queue", 
        "historical_data_queue",
        "dead_letter_queue"
      )

      requiredExchanges.foreach { exchange =>
        Try(channel.exchangeDeclarePassive(exchange)).recover {
          case _ => fail(s"Required exchange '$exchange' not found in test environment")
        }
      }

      requiredQueues.foreach { queue =>
        Try(channel.queueDeclarePassive(queue)).recover {
          case _ => fail(s"Required queue '$queue' not found in test environment")
        }
      }
    }
  }

  private def purgeTestQueues(): Unit = {
    testChannel.foreach { channel =>
      val queuesToPurge = List(
        "processed_events_queue",
        "recommendations_queue", 
        "historical_data_queue",
        "dead_letter_queue"
      )

      queuesToPurge.foreach { queue =>
        Try(channel.queuePurge(queue))
      }
    }
  }

  private def cleanupTestRabbitMQ(): Unit = {
    testChannel.foreach(_.close())
  }

  protected def publishTestMessage(exchange: String, routingKey: String, message: String): Unit = {
    testChannel.foreach { channel =>
      channel.basicPublish(exchange, routingKey, null, message.getBytes())
    }
  }

  protected def consumeTestMessage(queue: String): Option[String] = {
    testChannel.flatMap { channel =>
      Option(channel.basicGet(queue, true)).map(response => new String(response.getBody))
    }
  }
}

