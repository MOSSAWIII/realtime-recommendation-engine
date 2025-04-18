package core

import models._
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import config.RabbitMQConfig
import io.circe.parser._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
import org.slf4j.LoggerFactory

/**
 * Consumer responsable du traitement des événements utilisateur en temps réel.
 * 
 * Ce consumer écoute la queue USER_EVENTS qui contient les interactions utilisateur
 * brutes (clics, vues, etc.) et les transmet au Transformer pour générer
 * des recommandations en temps réel.
 * 
 * Rôle dans le pipeline:
 * [Événements user] → [RabbitMQ (StreamingConsumer)] → [Service de transformation]
 */
class StreamingConsumer(transformer: Transformer)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private var channel = RabbitMQConfig.createChannel()
  
  def start(): Unit = {
    logger.info("Démarrage du consumer d'événements utilisateur...")
    
    // Déclaration de la queue pour les événements utilisateur
    channel.queueDeclare(RabbitMQConfig.Queues.USER_EVENTS, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        // Décodage de l'événement JSON
        decode[UserEvent](message) match {
          case Right(event) =>
            logger.debug(s"Événement reçu: $event")
            
            // Transmission au transformer pour traitement
            transformer.processEvent(event).foreach { processedEvent =>
              logger.info(s"Événement traité pour l'utilisateur ${event.userId}")
              logger.debug(s"Détails: $processedEvent")
            }
            
          case Left(error) =>
            logger.error(s"Erreur de décodage: ${error.getMessage}", error)
        }
        
        // Accusé de réception
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        logger.warn(s"Consumer annulé: $consumerTag")
    }
    
    // Démarrage de la consommation
    channel.basicConsume(RabbitMQConfig.Queues.USER_EVENTS, false, deliverCallback, cancelCallback)
    logger.info("Consumer d'événements démarré avec succès")
  }
  
  def close(): Unit = {
    if (channel != null && channel.isOpen) {
      channel.close()
      logger.info("StreamingConsumer channel closed")
    }
  }

  def handleMessage(event: UserEvent): Option[ProcessedEvent] = {
    // Implémentation de la méthode
    val futureResult = transformer.processEvent(event)
    // Dans un test, on peut utiliser Await.result pour obtenir le résultat synchrone
    import scala.concurrent.Await
    import scala.concurrent.duration._
    try {
      Some(Await.result(futureResult, 5.seconds))
    } catch {
      case _: Exception => None
    }
  }
}