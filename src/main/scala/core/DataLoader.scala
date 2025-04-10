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

class DataLoader(
  userRepo: UserRepository,
  eventRepo: EventRepository,
  participationRepo: ParticipationRepository,
  friendsRepo: FriendsRepository,
  cacheManager: CacheManager
)(implicit ec: ExecutionContext) {
  
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
      users = Await.result(userRepo.findAll(), 30.seconds)
      println(s"Utilisateurs chargés: ${users.size}")
    } catch {
      case e: Exception => 
        println(s"Erreur lors du chargement des utilisateurs: ${e.getMessage}")
        users = Seq.empty
    }
    
    try {
      events = Await.result(eventRepo.findFutureEvents(), 30.seconds)
      println(s"Événements chargés: ${events.size}")
    } catch {
      case e: Exception => 
        println(s"Erreur lors du chargement des événements: ${e.getMessage}")
        events = Seq.empty
    }
    
    // Chargement des compteurs de participation
    participationCountByEvent = Map.empty
    if (events.nonEmpty) {
      try {
        val eventIds = events.map(_.eventId)
        participationCountByEvent = eventIds.map { eventId =>
          try {
            val count = Await.result(participationRepo.countByEventId(eventId), 10.seconds)
            eventId -> count
          } catch {
            case e: Exception =>
              println(s"Erreur lors du chargement des participations pour l'événement $eventId: ${e.getMessage}")
              eventId -> 0
          }
        }.toMap
      } catch {
        case e: Exception =>
          println(s"Erreur lors du chargement des compteurs de participation: ${e.getMessage}")
      }
    }
    
    // Chargement des amis
    friendsByUser = Map.empty
    if (users.nonEmpty) {
      try {
        friendsByUser = users.map { user =>
          try {
            val friendIds = Await.result(friendsRepo.findFriendsByUserId(user.userId), 10.seconds)
            user.userId -> friendIds
          } catch {
            case e: Exception =>
              println(s"Erreur lors du chargement des amis pour l'utilisateur ${user.userId}: ${e.getMessage}")
              user.userId -> Seq.empty[Long]
          }
        }.toMap
      } catch {
        case e: Exception =>
          println(s"Erreur lors du chargement des relations d'amitié: ${e.getMessage}")
      }
    }
    
    println(s"Données chargées: ${users.size} utilisateurs, ${events.size} événements")
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
    channel.queueDeclare("user_updates", true, false, false, null)
    
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
            println(s"Error decoding user message: ${error.getMessage}")
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        println(s"User consumer cancelled: $consumerTag")
    }
    
    channel.basicConsume("user_updates", false, deliverCallback, cancelCallback)
    userChannel = Some(channel)
  }
  
  private def startEventConsumer(): Unit = {
    val channel = RabbitMQConfig.createChannel()
    channel.queueDeclare("event_updates", true, false, false, null)
    
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
            println(s"Error decoding event message: ${error.getMessage}")
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = 
        println(s"Event consumer cancelled: $consumerTag")
    }
    
    channel.basicConsume("event_updates", false, deliverCallback, cancelCallback)
    eventChannel = Some(channel)
  }
  
  private def startParticipationConsumer(): Unit = {
    // Similar implementation for participation updates
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
            println(s"Error decoding participation message: ${error.getMessage}")
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume("participation_updates", false, deliverCallback, 
      consumerTag => println(s"Participation consumer cancelled: $consumerTag"))
    participationChannel = Some(channel)
  }
  
  private def startFriendshipConsumer(): Unit = {
    // Implementation for friendship updates
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
            println(s"Error decoding friendship message: ${error.getMessage}")
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