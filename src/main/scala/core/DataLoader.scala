package core

import models._
import repositories._
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import config.DatabaseConfig.db
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
  friendsRepo: FriendsRepository,
  cacheManager: CacheManager
)(implicit ec: ExecutionContext) {
  
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Helper method for detailed error logging
  private def logError(message: String, error: Throwable): Unit = {
    logger.error(s"$message: ${error.getMessage}")
    logger.debug("Stack trace:", error)
    
    // Also print to console for Docker logs visibility
    println(s"ERROR: $message: ${error.getMessage}")
    error.printStackTrace()
  }
  
  private def verifyDatabaseConnection(): Boolean = {
    try {
      logger.info("Verifying database connection...")
      val result = Await.result(db.run(sql"SELECT 1".as[Int]), 5.seconds)
      logger.info("Database connection verified successfully")
      true
    } catch {
      case e: Exception =>
        logError("Failed to verify database connection", e)
        false
    }
  }
  
  // Cache pour les données chargées
  private var users: Seq[User] = Seq.empty
  private var events: Seq[Event] = Seq.empty
  private var participationCountByEvent: Map[Long, Int] = Map.empty
  private var friendsByUser: Map[Long, Seq[Long]] = Map.empty
  
  // Initialize channels for different data types
  private var userChannel: Option[Channel] = None
  private var eventChannel: Option[Channel] = None
  private var participationChannel: Option[Channel] = None
  private var friendshipChannel: Option[Channel] = None
  
  def loadAll(): Unit = {
    try {
      logger.info("Loading users from database...")
      users = Await.result(userRepo.findAll(), 30.seconds)
      logger.info(s"Users loaded successfully: ${users.size} records")
    } catch {
      case e: java.sql.SQLException => 
        logError("Database error while loading users", e)
        users = Seq.empty
      case e: java.util.concurrent.TimeoutException =>
        logError("Timeout waiting for user data", e)
        users = Seq.empty
      case e: Exception => 
        logError("Unexpected error while loading users", e)
        users = Seq.empty
    }
    
    try {
      logger.info("Loading future events from database...")
      events = Await.result(eventRepo.findFutureEvents(), 30.seconds)
      logger.info(s"Events loaded successfully: ${events.size} records")
    } catch {
      case e: java.sql.SQLException => 
        logError("Database error while loading events", e)
        events = Seq.empty
      case e: java.util.concurrent.TimeoutException =>
        logError("Timeout waiting for event data", e)
        events = Seq.empty
      case e: Exception => 
        logError("Unexpected error while loading events", e)
        events = Seq.empty
    }
    
    // Enhanced logging for participation counts
    participationCountByEvent = Map.empty
    if (events.nonEmpty) {
      logger.info(s"Loading participation counts for ${events.size} events...")
      try {
        val eventIds = events.map(_.eventId)
        participationCountByEvent = eventIds.map { eventId =>
          try {
            val count = Await.result(participationRepo.countByEventId(eventId), 10.seconds)
            eventId -> count
          } catch {
            case e: Exception =>
              logError(s"Failed to load participation count for event $eventId", e)
              eventId -> 0
          }
        }.toMap
        logger.info(s"Loaded participation counts for ${participationCountByEvent.size} events")
      } catch {
        case e: Exception =>
          logError("Failed to load participation counts", e)
      }
    } else {
      logger.info("No events to load participation counts for")
    }
    
    // Friendship loading with enhanced error logging
    friendsByUser = Map.empty
    if (users.nonEmpty) {
      logger.info(s"Loading friendships for ${users.size} users...")
      try {
        friendsByUser = users.map { user =>
          try {
            val friendIds = Await.result(friendsRepo.findFriendsByUserId(user.userId), 10.seconds)
            if (friendIds.nonEmpty) {
              logger.debug(s"User ${user.userId} has ${friendIds.size} friends")
            }
            user.userId -> friendIds
          } catch {
            case e: Exception =>
              logError(s"Failed to load friends for user ${user.userId}", e)
              user.userId -> Seq.empty[Long]
          }
        }.toMap
        logger.info(s"Loaded friendship data for ${friendsByUser.size} users")
      } catch {
        case e: Exception =>
          logError("Failed to load friendship relationships", e)
      }
    } else {
      logger.info("No users to load friendship data for")
    }

    // Final summary with diagnostic information
    val emptyFriendships = friendsByUser.count(_._2.isEmpty)
    logger.info(s"Data loading complete: ${users.size} users, ${events.size} events, ${participationCountByEvent.size} participation counts")
    logger.info(s"Friendship stats: ${friendsByUser.size} users mapped, $emptyFriendships users with no friends")
  }
  
  // Méthode asynchrone pour le chargement
  def loadAllAsync(): Future[Unit] = {
    for {
      loadedUsers <- userRepo.findAll()
      loadedEvents <- eventRepo.findFutureEvents()
      _ <- loadParticipationCounts(loadedEvents)
      _ <- loadFriendships(loadedUsers)
    } yield {
      users = loadedUsers
      events = loadedEvents
      println(s"Données chargées: ${users.size} utilisateurs, ${events.size} événements")
    }
  }
  
  private def loadParticipationCounts(eventList: Seq[Event]): Future[Unit] = {
    val eventIds = eventList.map(_.eventId)
    
    Future.sequence(eventIds.map { eventId =>
      participationRepo.countByEventId(eventId).map(count => eventId -> count)
    }).map { counts =>
      participationCountByEvent = counts.toMap
    }
  }
  
  private def loadFriendships(userList: Seq[User]): Future[Unit] = {
    Future.sequence(userList.map { user =>
      friendsRepo.findFriendsByUserId(user.userId).map(friends => user.userId -> friends)
    }).map { friendships =>
      friendsByUser = friendships.toMap
    }
  }
  
  // Getters pour les données chargées
  def getUsers(): List[User] = users.toList
  
  def getEvents(): List[Event] = events.toList
  
  def getParticipationCountForEvent(eventId: Long): Int = {
    participationCountByEvent.getOrElse(eventId, 0)
  }
  
  def getFriendsForUser(userId: Long): List[Long] = {
    friendsByUser.getOrElse(userId, Seq.empty).toList
  }
  
  // Méthode utilitaire pour extraire les mots-clés d'un événement
  def getEventKeywords(eventId: Long): Map[Int, Int] = {
    events.find(_.eventId == eventId)
      .map(_.keywordFrequencies)
      .getOrElse(Map.empty)
  }
  
  // Start the data stream consumers
  def startConsumers(): Unit = {
    startUserConsumer()
    startEventConsumer()
    startParticipationConsumer()
    startFriendshipConsumer()
    println("All data stream consumers started successfully")
  }
  
  private def startUserConsumer(): Unit = {
    val channel = RabbitMQConfig.createChannel()
    channel.queueDeclare(RabbitMQConfig.Queues.USER_INTERACTIONS, true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[User](message) match {
          case Right(user) =>
            // Update cache and database
            userRepo.upsert(user)
            cacheManager.cacheUser(user)
            println(s"Received user update for ${user.userId}")
          case Left(error) =>
            logError("Error decoding user message", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        println(s"User consumer cancelled: $consumerTag")
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
            // Update cache and database
            eventRepo.upsert(event)
            cacheManager.cacheEvent(event)
            println(s"Received event update for ${event.eventId}")
          case Left(error) =>
            logError("Error decoding event message", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        println(s"Event consumer cancelled: $consumerTag")
    }
    
    channel.basicConsume(RabbitMQConfig.Queues.EVENT_UPDATES, false, deliverCallback, cancelCallback)
    eventChannel = Some(channel)
  }
  
  private def startParticipationConsumer(): Unit = {
    val channel = RabbitMQConfig.createChannel()
    channel.queueDeclare("participation_updates", true, false, false, null)
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        decode[EventParticipation](message) match {
          case Right(participation) =>
            participationRepo.upsert(participation)
            // Invalidate related caches
            cacheManager.invalidateEvent(participation.eventId)
            println(s"Received participation update for event ${participation.eventId}")
          case Left(error) =>
            logError("Error decoding participation message", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume("participation_updates", false, deliverCallback, 
      consumerTag => println(s"Participation consumer cancelled: $consumerTag"))
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
            // Invalidate user recommendations cache
            cacheManager.invalidateUserRecommendations(friendship.userId)
            println(s"Received friendship update for user ${friendship.userId}")
          case Left(error) =>
            logError("Error decoding friendship message", error)
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume("friendship_updates", false, deliverCallback,
      consumerTag => println(s"Friendship consumer cancelled: $consumerTag"))
    friendshipChannel = Some(channel)
  }
  
  def close(): Unit = {
    // Close all channels
    userChannel.foreach(_.close())
    eventChannel.foreach(_.close())
    participationChannel.foreach(_.close())
    friendshipChannel.foreach(_.close())
  }
}