package core

import models._
import repositories._
import config.RabbitMQManager
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
 * [Mises à jour de données] → [RabbitMQ (ConsumerManager)] → [BD]
 */
class ConsumerManager(
  userRepo: UserRepository,
  eventRepo: EventRepository,
  participationRepo: ParticipationRepository,
  friendsRepo: FriendsRepository
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
    val channel = RabbitMQManager.createChannel("user-consumer")
    channel.queueDeclare(RabbitMQManager.Queues.USER_INTERACTIONS, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[User](message) match {
          case Right(user) =>
            userRepo.upsert(user)
            logger.info(s"Received user update for ${user.userId}")
          case Left(error) =>
            logger.error(s"Error decoding user message: ${error.getMessage}", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume(RabbitMQManager.Queues.USER_INTERACTIONS, false, deliverCallback,
      consumerTag => logger.warn(s"User consumer cancelled: $consumerTag"))
    userChannel = Some(channel)
  }
  
  private def startEventConsumer(): Unit = {
    val channel = RabbitMQManager.createChannel("event-consumer")
    channel.queueDeclare(RabbitMQManager.Queues.EVENT_UPDATES, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[Event](message) match {
          case Right(event) => 
            eventRepo.upsert(event)
            logger.info(s"Received event update for ${event.id.getOrElse("unknown")}")
          case Left(error) =>
            logger.error(s"Error decoding event message: ${error.getMessage}", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume(RabbitMQManager.Queues.EVENT_UPDATES, false, deliverCallback,
      consumerTag => logger.warn(s"Event consumer cancelled: $consumerTag"))
    eventChannel = Some(channel)
  }
  
  private def startParticipationConsumer(): Unit = {
    val channel = RabbitMQManager.createChannel("participation-consumer")
    channel.queueDeclare(RabbitMQManager.Queues.PARTICIPATION_UPDATES, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[EventParticipation](message) match {
          case Right(participation) =>
            participationRepo.upsert(participation)
            logger.info(s"Received participation update for event ${participation.eventId}")
          case Left(error) =>
            logger.error(s"Error decoding participation message: ${error.getMessage}", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume(RabbitMQManager.Queues.PARTICIPATION_UPDATES, false, deliverCallback,
      consumerTag => logger.warn(s"Participation consumer cancelled: $consumerTag"))
    participationChannel = Some(channel)
  }
  
  private def startFriendshipConsumer(): Unit = {
    val channel = RabbitMQManager.createChannel("friendship-consumer")
    channel.queueDeclare(RabbitMQManager.Queues.FRIENDSHIP_UPDATES, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[UserFriends](message) match {
          case Right(friendship) =>
            friendsRepo.upsert(friendship)
            logger.info(s"Received friendship update for user ${friendship.userId}")
          case Left(error) =>
            logger.error(s"Error decoding friendship message: ${error.getMessage}", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume(RabbitMQManager.Queues.FRIENDSHIP_UPDATES, false, deliverCallback,
      consumerTag => logger.warn(s"Friendship consumer cancelled: $consumerTag"))
    friendshipChannel = Some(channel)
  }
  
  def close(): Unit = {
    RabbitMQManager.closeChannel("user-consumer")
    RabbitMQManager.closeChannel("event-consumer")
    RabbitMQManager.closeChannel("participation-consumer")
    RabbitMQManager.closeChannel("friendship-consumer")
    logger.info("All consumer channels closed")
  }
}

