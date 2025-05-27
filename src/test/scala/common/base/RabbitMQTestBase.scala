package common.base

import com.rabbitmq.client.Channel
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import common.utils.{TestResourceManager}
import infrastructure.messaging.rabbitmq.RabbitMQManager

/**
 * Base de test pour les tests utilisant RabbitMQ
 */
trait RabbitMQTestBase extends IsolatedTestBase {
  
  // Noms uniques de ressources pour ce test
  val testExchange: String = s"test_exchange_${testId}"
  
  // Canaux RabbitMQ pour ce test
  private var testChannels = scala.collection.mutable.Map.empty[String, Channel]
  
  /**
   * Crée un canal RabbitMQ unique pour ce test
   */
  protected def createTestChannel(name: String): Channel = {
    val channel = RabbitMQManager.createChannel(s"test_${name}_${testId}")
    testChannels += (name -> channel)
    channel
  }
  
  /**
   * Crée une file d'attente isolée pour ce test
   */
  protected def createIsolatedQueue(channel: Channel, prefix: String = "test"): String = {
    val queueName = TestResourceManager.uniqueQueueName(prefix)
    channel.queueDeclare(queueName, true, false, true, null)
    queueName
  }
  
  /**
   * Attend qu'un message soit reçu avec timeout
   */
  protected def waitForMessage[T](promise: Promise[T], timeout: Duration = 5.seconds): Option[T] = {
    try {
      Some(Await.result(promise.future, timeout))
    } catch {
      case _: Exception => None
    }
  }
  
  override def afterEach(): Unit = {
    // Fermer et nettoyer tous les canaux créés pour ce test
    testChannels.foreach { case (name, channel) =>
      try {
        if (channel.isOpen) {
          channel.close()
        }
      } catch {
        case e: Exception => 
          println(s"Error closing channel $name: ${e.getMessage}")
      }
    }
    testChannels.clear()
    
    super.afterEach()
  }
}

