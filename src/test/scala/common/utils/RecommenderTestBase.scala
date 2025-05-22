package testutil

import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.{ExecutionContext, Promise, Future, Await}
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch

import core.Recommender
import models.{Recommendation, ProcessedEvent}
import common.utils.RabbitMQManager
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import java.nio.charset.StandardCharsets
import io.circe.parser._
import io.circe.generic.auto._

abstract class RecommenderTestBase extends AnyWordSpec with BeforeAndAfterEach with Matchers {
  
  // Components
  protected var mockDataManager: MockDataManager = _
  protected var recommender: Recommender = _
  
  // Configuration
  protected implicit val ec: ExecutionContext = ExecutionContext.global
  
  // Recommendation capture
  protected var recommendationPromise: Promise[Recommendation] = _
  protected var consumerTag: String = _
  
  /**
   * Setup recommender with mock data manager
   */
  protected def setupRecommender(latchSize: Int = 0): Unit = {
    val latch = if (latchSize > 0) new CountDownLatch(latchSize) else null
    mockDataManager = new MockDataManager(latch)
    recommender = new Recommender(mockDataManager)
    recommender.start()
    
    // Reset promise
    recommendationPromise = Promise[Recommendation]()
    
    // Setup listener for recommendations
    setupRecommendationListener()
  }
  
  /**
   * Setup listener for recommendations
   */
  protected def setupRecommendationListener(): Unit = {
    val channel = RabbitMQManager.createChannel("recommender-test-listener")
    channel.queueDeclare(RabbitMQManager.Queues.RECOMMENDATIONS, true, false, false, null)
    channel.queuePurge(RabbitMQManager.Queues.RECOMMENDATIONS)
    
    val callback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[Recommendation](message) match {
          case Right(recommendation) => 
            if (!recommendationPromise.isCompleted) {
              recommendationPromise.success(recommendation)
            }
          case Left(error) => 
            println(s"Failed to decode recommendation: $error")
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    consumerTag = channel.basicConsume(
      RabbitMQManager.Queues.RECOMMENDATIONS, 
      false,  // manual ack
      callback,
      new CancelCallback {
        override def handle(consumerTag: String): Unit = {}
      }
    )
  }
  
  /**
   * Get recommendation with timeout
   */
  protected def waitForRecommendation(timeout: Duration = 10.seconds): Recommendation = {
    try {
      Await.result(recommendationPromise.future, timeout)
    } catch {
      case _: java.util.concurrent.TimeoutException => 
        fail(s"Timed out waiting for recommendation after ${timeout.toSeconds} seconds")
    }
  }
  
  /**
   * Clean up resources
   */
  override def afterEach(): Unit = {
    if (recommender != null) {
      recommender.close()
    }
    
    // Cancel consumer
    try {
      val channel = RabbitMQManager.getChannel("recommender-test-listener")
      if (channel != null && channel.isOpen && consumerTag != null) {
        channel.basicCancel(consumerTag)
      }
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
    
    super.afterEach()
  }
}

