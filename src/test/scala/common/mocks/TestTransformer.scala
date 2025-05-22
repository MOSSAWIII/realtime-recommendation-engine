package integration.streaming

import core.{Transformer, StreamingConsumer}
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import models.{UserEvent, ProcessedEvent, RecommendationRequest}
import io.circe.parser.decode
import io.circe.syntax._

import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, Promise}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure, Try}
import org.slf4j.LoggerFactory
import common.utils.RabbitMQManager

class TestTransformer(
  dataManager: DataManager = null,
  queueName: String,
  processPromise: scala.concurrent.Promise[Boolean]
)(implicit ec: ExecutionContext) extends Transformer(dataManager) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  private var consumerTag: String = _
  private var consumeChannel: com.rabbitmq.client.Channel = _
  
  /**
   * Configure pour consommer directement depuis la queue de test
   * au lieu de l'exchange standard
   */
  override def start(): Unit = {
    logger.info(s"Starting test transformer for queue: $queueName")
    
    // Create a consumer channel
    consumeChannel = RabbitMQManager.createChannel("transformer-test-consumer")
    
    // Ensure the queue exists
    consumeChannel.queueDeclare(queueName, true, false, false, null)
    
    // Set QoS to avoid overwhelming the consumer
    consumeChannel.basicQos(10)
    
    // Create a delivery callback that processes messages
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        logger.debug(s"Test transformer received message: $message")
        
        try {
          decode[UserEvent](message) match {
            case Right(event) =>
              logger.info(s"Processing event for user ${event.userId}")
              
              processEvent(event).onComplete {
                case Success(processedEvent) => 
                  logger.info(s"Successfully processed event for user ${event.userId}")
                  Try(consumeChannel.basicAck(delivery.getEnvelope.getDeliveryTag, false))
                    .recover { case e => logger.error(s"Failed to acknowledge message: ${e.getMessage}", e) }
                  
                  // Signal that processing completed successfully
                  if (!processPromise.isCompleted) {
                    processPromise.success(true)
                  }
                  
                case Failure(ex) => 
                  logger.error(s"Error processing event: ${ex.getMessage}", ex)
                  Try(consumeChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false))
                    .recover { case e => logger.error(s"Failed to negative-acknowledge message: ${e.getMessage}", e) }
                  
                  if (!processPromise.isCompleted) {
                    processPromise.failure(ex)
                  }
              }
              
            case Left(error) =>
              logger.error(s"Failed to decode message: ${error.getMessage}")
              Try(consumeChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false))
                .recover { case e => logger.error(s"Failed to negative-acknowledge message: ${e.getMessage}", e) }
                
              if (!processPromise.isCompleted) {
                processPromise.failure(new RuntimeException(s"Failed to decode message: ${error.getMessage}"))
              }
          }
        } catch {
          case e: Exception => 
            logger.error(s"Unexpected error processing message: ${e.getMessage}", e)
            Try(consumeChannel.basicNack(delivery.getEnvelope.getDeliveryTag, false, false))
              .recover { case e => logger.error(s"Failed to negative-acknowledge message: ${e.getMessage}", e) }
        }
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.info(s"Consumer cancelled: $consumerTag")
      }
    }
    
    // Start consuming with manual ack mode
    consumerTag = consumeChannel.basicConsume(queueName, false, deliverCallback, cancelCallback)
    logger.info(s"Test transformer started and listening to queue: $queueName with tag: $consumerTag")
  }
  
  /**
   * Ferme proprement le canal de consommation
   */
  override def close(): Unit = {
    try {
      // Add a small delay to allow in-progress operations to complete
      Thread.sleep(100)
      
      if (consumeChannel != null && consumeChannel.isOpen) {
        try {
          // Cancel consumer if possible
          if (consumerTag != null && !consumerTag.isEmpty) {
            logger.debug(s"Cancelling consumer with tag: $consumerTag")
            Try(consumeChannel.basicCancel(consumerTag))
              .recover { case e => logger.warn(s"Error canceling consumer: ${e.getMessage}") }
          }
          
          // Close channel
          RabbitMQManager.closeChannel("transformer-test-consumer")
        } catch {
          case e: Exception => 
            logger.warn(s"Error during consumer cleanup: ${e.getMessage}", e)
        }
      }
      
      // Call parent close to clean up publish channels
      super.close()
      
      logger.info("Test transformer closed")
    } catch {
      case e: Exception =>
        logger.error(s"Error closing transformer: ${e.getMessage}", e)
    }
  }
  
  /**
   * Ne publie pas réellement les événements traités pendant les tests
   */
  override def publishProcessedEvent(event: ProcessedEvent): Unit = {
    // Do nothing - don't publish to RabbitMQ during tests
    logger.debug(s"Test transformer - not publishing processed event: $event")
  }
  
  /**
   * Pour les tests, nous pouvons capturer les demandes de recommandation 
   * au lieu de les publier
   */
  var recommendationRequests: List[RecommendationRequest] = List.empty
  
  override def publishRecommendationRequest(request: RecommendationRequest): Unit = {
    recommendationRequests = request :: recommendationRequests
    logger.debug(s"Test transformer - captured recommendation request: $request")
  }
}

/** 
 * Classe de compatibilité à supprimer quand tous les tests auront été migrés
 * @deprecated Utiliser directement TestTransformer
 */
@deprecated("Utiliser TestTransformer directement", "Version 2.0")
class TestStreamingConsumer(
  transformer: Transformer, 
  queueName: String,
  processPromise: Promise[Boolean]
)(implicit ec: ExecutionContext) extends StreamingConsumer(transformer) {
  
  // Utiliser TestTransformer en interne pour la compatibilité
  private val testTransformer = new TestTransformer(null, queueName, processPromise)
  
  override def start(): Unit = {
    testTransformer.start()
  }
  
  override def close(): Unit = {
    testTransformer.close()
  }
}

// Dans le test:
"process a single event successfully" in {
  val testUserId = 101L
  val processPromise = Promise[Boolean]()
  
  // Use a real latch to track completion
  val latch = new CountDownLatch(1)
  val mockDataManager = new MockDataManager(latch)
  
  // Créer directement un TestTransformer avec MockDataManager
  val transformer = new TestTransformer(mockDataManager, testQueueName, processPromise)
  
  // Start the transformer
  transformer.start()
  
  // Le reste du test reste similaire...
}

