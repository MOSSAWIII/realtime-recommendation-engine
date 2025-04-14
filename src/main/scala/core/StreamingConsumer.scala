package core

import models._
import com.rabbitmq.client.{DeliverCallback, CancelCallback}
import config.RabbitMQConfig
import io.circe.parser._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}

class StreamingConsumer(transformer: Transformer)(implicit ec: ExecutionContext) {
  private var channel = RabbitMQConfig.createChannel()
  
  def start(): Unit = {
    println("Démarrage du consumer d'événements utilisateur...")
    
    // Déclaration de la queue pour les événements utilisateur
    channel.queueDeclare(RabbitMQConfig.Queues.USER_EVENTS, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        // Décodage de l'événement JSON
        decode[UserEvent](message) match {
          case Right(event) =>
            println(s"Événement reçu: $event")
            
            // Transmission au transformer pour traitement
            transformer.processEvent(event).foreach { processedEvent =>
              println(s"Événement traité: $processedEvent")
            }
            
          case Left(error) =>
            println(s"Erreur de décodage: ${error.getMessage}")
        }
        
        // Accusé de réception
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        println(s"Consumer annulé: $consumerTag")
    }
    
    // Démarrage de la consommation
    channel.basicConsume(RabbitMQConfig.Queues.USER_EVENTS, false, deliverCallback, cancelCallback)
    println("Consumer d'événements démarré avec succès")
  }
  
  def close(): Unit = {
    if (channel != null && channel.isOpen) {
      channel.close()
    }
  }
}