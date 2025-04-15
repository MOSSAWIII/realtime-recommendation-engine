package core

import models._
import repositories._
import config.RabbitMQConfig
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback}
import io.circe.parser._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext

/**
 * Gestionnaire des consumers pour les mises à jour de données de référence.
 * 
 * Ce gestionnaire s'occupe des queues qui mettent à jour les données de référence
 * (utilisateurs, événements, participations, amitiés) mais ne traite PAS les
 * événements utilisateur en temps réel (qui sont gérés par StreamingConsumer).
 * 
 * Rôle dans le pipeline:
 * [Mises à jour de données] → [RabbitMQ (ConsumerManager)] → [Cache + BD]
 */
class ConsumerManager(
  userRepo: UserRepository,
  eventRepo: EventRepository,
  participationRepo: ParticipationRepository,
  friendsRepo: FriendsRepository,
  cacheManager: CacheManager
)(implicit ec: ExecutionContext) {

  private val logger = LoggerFactory.getLogger(getClass)
  
  private var userChannel: Option[Channel] = None
  private var eventChannel: Option[Channel] = None
  private var participationChannel: Option[Channel] = None
  private var friendshipChannel: Option[Channel] = None
  
  def startAllConsumers(): Unit = {
    startUserConsumer()
    startEventConsumer()
    startParticipationConsumer()
    startFriendshipConsumer()
    logger.info("All reference data consumers started successfully")
  }
  
  private def startUserConsumer(): Unit = {
    val channel = RabbitMQConfig.createChannel()
    channel.queueDeclare(RabbitMQConfig.Queues.USER_INTERACTIONS, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[User](message) match {
          case Right(user) =>
            userRepo.upsert(user)
            cacheManager.cacheUser(user)
            logger.info(s"Received user update for ${user.userId}")
          case Left(error) =>
            logger.error(s"Error decoding user message: ${error.getMessage}", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        logger.warn(s"User consumer cancelled: $consumerTag")
    }
    
    channel.basicConsume(RabbitMQConfig.Queues.USER_INTERACTIONS, false, deliverCallback, cancelCallback)
    userChannel = Some(channel)
  }
  
  // Les autres méthodes restent similaires, mais utilisent les constantes de RabbitMQConfig.Queues
  
  def close(): Unit = {
    // Close all channels
    userChannel.foreach { ch => 
      if (ch.isOpen) ch.close() 
      logger.info("User channel closed")
    }
    eventChannel.foreach { ch => 
      if (ch.isOpen) ch.close() 
      logger.info("Event channel closed")
    }
    participationChannel.foreach { ch => 
      if (ch.isOpen) ch.close() 
      logger.info("Participation channel closed")
    }
    friendshipChannel.foreach { ch => 
      if (ch.isOpen) ch.close() 
      logger.info("Friendship channel closed")
    }
  }
}

