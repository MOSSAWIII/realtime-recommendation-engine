package integration.streaming

import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.slf4j.LoggerFactory
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import integration.TestConfig
import com.rabbitmq.client.Channel
import java.nio.charset.StandardCharsets
import config.RabbitMQManager
import io.circe.syntax._
import io.circe.Encoder

trait StreamingTestSupport extends Eventually with ScalaFutures {
  protected val logger = LoggerFactory.getLogger(getClass)
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
  
  // More aggressive patience config for async tests
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(15, Seconds)),
    interval = scaled(Span(150, Millis))
  )
  
  /**
   * Publish a batch of messages to a RabbitMQ queue
   * 
   * @param queueName The target queue name
   * @param messages The messages to publish
   * @param componentName The name for the publisher channel
   * @param addDelay Whether to add a delay between messages
   */
  def publishMessages[T: Encoder](
    queueName: String, 
    messages: Seq[T], 
    componentName: String = "test-publisher",
    addDelay: Boolean = true
  ): Unit = {
    val channel = RabbitMQManager.createChannel(componentName)
    
    try {
      // Declare queue to ensure it exists
      channel.queueDeclare(queueName, true, false, false, null)
      
      logger.info(s"Publishing ${messages.size} messages to queue $queueName")
      
      messages.foreach { message =>
        val messageJson = message.asJson.noSpaces
        channel.basicPublish("", queueName, null, messageJson.getBytes(StandardCharsets.UTF_8))
        
        if (addDelay) {
          // Small delay to prevent overwhelming the broker
          Thread.sleep(50) // Default delay
        }
      }
      
      logger.info(s"Finished publishing ${messages.size} messages to $queueName")
    } catch {
      case e: Exception =>
        logger.error(s"Error publishing messages to queue $queueName: ${e.getMessage}", e)
        throw e
    }
  }
  
  /**
   * Wait for a latch with logging
   */
  def waitForLatch(latch: CountDownLatch, timeout: Duration, description: String): Boolean = {
    logger.info(s"Waiting for $description (timeout: ${timeout.toSeconds} seconds)")
    val success = latch.await(timeout.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    if (success) {
      logger.info(s"$description completed successfully")
    } else {
      logger.warn(s"Timed out waiting for $description, ${latch.getCount} remaining")
    }
    success
  }
  
  /**
   * Purge and delete a queue
   */
  def cleanupQueue(queueName: String): Unit = {
    val channel = RabbitMQManager.createChannel("cleanup")
    try {
      // First purge to avoid messages hanging around
      channel.queuePurge(queueName)
      // Then delete the queue
      channel.queueDelete(queueName)
      logger.info(s"Queue $queueName purged and deleted")
    } catch {
      case e: Exception => 
        logger.warn(s"Error cleaning up queue $queueName: ${e.getMessage}", e)
    } finally {
      RabbitMQManager.closeChannel("cleanup")
    }
  }
}

