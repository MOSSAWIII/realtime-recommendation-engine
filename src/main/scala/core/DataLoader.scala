package core

import models._
import repositories._
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import config.RabbitMQConfig
import com.rabbitmq.client.{Channel, DeliverCallback, CancelCallback}
import io.circe.parser._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory

class DataLoader(
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
  
  // Chargement asynchrone de toutes les données
  def loadAllAsync(): Future[Unit] = {
    // Modified to remove cache references
    val futures = Seq(
      userRepo.findAll().map { users =>
        logger.info(s"Loaded ${users.size} users from database")
      }.recover { case ex =>
        logger.error("Failed to load users", ex)
        throw ex
      },
      
      eventRepo.findFutureEvents().map { events =>
        logger.info(s"Loaded ${events.size} events from database")
      }.recover { case ex =>
        logger.error("Failed to load events", ex)
        throw ex
      },
      
      participationRepo.findAll().map { participations =>
        logger.info(s"Loaded ${participations.size} participations from database")
      }.recover { case ex =>
        logger.error("Failed to load participations", ex)
        throw ex
      },
      
      friendsRepo.findAll().map { friendships =>
        logger.info(s"Loaded ${friendships.size} friendships from database")
      }.recover { case ex =>
        logger.error("Failed to load friendships", ex)
        throw ex
      }
    )
    
    Future.sequence(futures).map(_ => ())
  }
  
  // Méthode synchrone qui attend le chargement asynchrone
  def loadAll(): Unit = {
    try {
      Await.result(loadAllAsync(), 2.minutes)
      logger.info("All data loaded successfully")
    } catch {
      case ex: Exception =>
        logger.error("Failed to load all data", ex)
        throw ex
    }
  }
  
  // Méthode utilitaire pour extraire les mots-clés d'un événement
  def getEventKeywords(eventId: Long): Future[Map[String, Int]] = {
    eventRepo.findById(eventId).flatMap(eventOpt => 
      Future.successful(eventOpt.map(_.keywordFrequencies).getOrElse(Map.empty))
    )
  }
  
  // Start the data stream consumers
  def startConsumers(): Unit = {
    startUserConsumer()
    startEventConsumer()
    startParticipationConsumer()
    startFriendshipConsumer()
    logger.info("All data stream consumers started successfully")
  }
  
  private def startUserConsumer(): Unit = {
    val channel = RabbitMQConfig.createChannel()
    channel.queueDeclare(RabbitMQConfig.Queues.USER_INTERACTIONS, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[User](message) match {
          case Right(user) =>
            // Update database only (removed cache update)
            userRepo.upsert(user)
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
  
  private def startEventConsumer(): Unit = {
    val channel = RabbitMQConfig.createChannel()
    channel.queueDeclare(RabbitMQConfig.Queues.EVENT_UPDATES, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[Event](message) match {
          case Right(event) =>
            // Update database only (removed cache update)
            eventRepo.upsert(event)
            logger.info(s"Received event update for ${event.id.getOrElse("unknown")}")
          case Left(error) =>
            logger.error(s"Error decoding event message: ${error.getMessage}", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        logger.warn(s"Event consumer cancelled: $consumerTag")
    }
    
    channel.basicConsume(RabbitMQConfig.Queues.EVENT_UPDATES, false, deliverCallback, cancelCallback)
    eventChannel = Some(channel)
  }
  
  private def startParticipationConsumer(): Unit = {
    val channel = RabbitMQConfig.createChannel()
    channel.queueDeclare(RabbitMQConfig.Queues.PARTICIPATION_UPDATES, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[EventParticipation](message) match {
          case Right(participation) =>
            participationRepo.upsert(participation)
            // Removed cache invalidation
            logger.info(s"Received participation update for event ${participation.eventId}")
          case Left(error) =>
            logger.error(s"Error decoding participation message: ${error.getMessage}", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume(RabbitMQConfig.Queues.PARTICIPATION_UPDATES, false, deliverCallback, 
      consumerTag => logger.warn(s"Participation consumer cancelled: $consumerTag"))
    participationChannel = Some(channel)
  }
  
  private def startFriendshipConsumer(): Unit = {
    val channel = RabbitMQConfig.createChannel()
    channel.queueDeclare("friendship_updates", true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[UserFriends](message) match {
          case Right(friendship) =>
            friendsRepo.upsert(friendship)
            // Removed cache invalidation
            logger.info(s"Received friendship update for user ${friendship.userId}")
          case Left(error) =>
            logger.error(s"Error decoding friendship message: ${error.getMessage}", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume("friendship_updates", false, deliverCallback,
      consumerTag => logger.warn(s"Friendship consumer cancelled: $consumerTag"))
    friendshipChannel = Some(channel)
  }
  
  def close(): Unit = {
    userChannel.foreach(ch => if (ch.isOpen) ch.close())
    eventChannel.foreach(ch => if (ch.isOpen) ch.close())
    participationChannel.foreach(ch => if (ch.isOpen) ch.close())
    friendshipChannel.foreach(ch => if (ch.isOpen) ch.close())
    logger.info("DataLoader channels closed")
  }
}