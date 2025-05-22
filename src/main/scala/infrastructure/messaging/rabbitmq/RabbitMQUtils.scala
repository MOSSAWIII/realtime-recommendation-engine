package infrastructure.messaging.rabbitmq

import org.slf4j.LoggerFactory
import com.rabbitmq.client.{CancelCallback, Channel}
import infrastructure.messaging.rabbitmq.RabbitMQManager

/**
 * Utilitaires pour RabbitMQ utilisés dans l'architecture simplifiée
 */
object RabbitMQUtils {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Crée une fonction de purge pour nettoyer une queue spécifiée
   * 
   * @param queueName Nom de la queue à purger
   * @return Nombre de messages purgés
   */
  def purgeQueue(queueName: String): Int = {
    val channel = RabbitMQManager.createChannel("purge-channel")
    try {
      val count = channel.queuePurge(queueName).getMessageCount
      logger.info(s"Purged $count messages from queue $queueName")
      count
    } finally {
      RabbitMQManager.closeChannel("purge-channel")
    }
  }
  
  /**
   * Create a CancelCallback that attempts to restart the consumer after a delay
   * 
   * @param consumerName Name of the consumer for logging
   * @param channelName Channel name for recreation
   * @param restartFunc Function to call for restarting the consumer
   * @return CancelCallback implementation
   */
  def createRestartableCallback(consumerName: String, channelName: String, restartFunc: () => Unit): CancelCallback = {
    new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"$consumerName consumer cancelled: $consumerTag")
        
        // Try to reconnect in a separate thread
        new Thread(() => {
          try {
            logger.info(s"Attempting to restart $consumerName consumer in 5 seconds...")
            Thread.sleep(5000)
            restartFunc()
          } catch {
            case e: Exception =>
              logger.error(s"Failed to restart $consumerName consumer: ${e.getMessage}", e)
          }
        }).start()
      }
    }
  }
}

