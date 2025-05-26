package common.fixtures

import domain.models._
import java.time.Instant
import java.util.UUID
import scala.util.Random

/**
 * Centralized factory for generating test data
 */
object TestDataFactory {
  // Unique ID generators
  def uniqueId(): Long = System.currentTimeMillis()
  def uniqueString(): String = UUID.randomUUID().toString
  
  // Create test users
  def createUser(
    id: Long = uniqueId(),
    name: String = s"User ${uniqueId()}",
    emailAddress: String = s"user_${uniqueId()}@example.com",
    locale: String = "en_US",
    birthYear: Option[Int] = Some(1990),
    gender: Option[String] = Some("M"),
    location: Option[String] = Some("Paris, France"),
    timezone: Option[Int] = Some(1),
    isGenerated: Boolean = false
  ): User = {
    User(
      userId = id,
      name = Some(name),
      emailAddress = Some(emailAddress),
      locale = locale,
      birthYear = birthYear,
      gender = gender,
      location = location,
      timezone = timezone,
      isGenerated = isGenerated
    )
  }
  
  // Create test events
  def createEvent(
    id: Option[Long] = Some(uniqueId()), 
    name: String = s"Event ${uniqueId()}", 
    description: Option[String] = Some(s"Description for Event ${uniqueId()}"),
    startTime: Long = System.currentTimeMillis() + 3600000,
    endTime: Option[Long] = Some(System.currentTimeMillis() + 7200000),
    location: Option[String] = Some("Test Location"),
    createdBy: Long = uniqueId(),
    status: String = "active",
    participants: Set[Long] = Set.empty
  ): Event = {
    Event(
      id = id,
      name = name,
      description = description,
      startTime = startTime,
      endTime = endTime,
      location = location,
      createdBy = createdBy,
      status = status,
      participants = participants
    )
  }
  
  // Create test user events
  def createUserEvent(
    userId: Long = uniqueId(), 
    eventType: String = "view", 
    itemId: Option[Long] = Some(uniqueId()),
    properties: Option[Map[String, String]] = Some(Map("test" -> "true")),
    timestamp: Long = System.currentTimeMillis()
  ): UserEvent = {
    UserEvent(
      userId = userId,
      eventType = eventType,
      itemId = itemId,
      properties = properties,
      timestamp = timestamp
    )
  }
  
  // Create test processed events
  def createProcessedEvent(
    id: Option[Long] = Some(uniqueId()),
    userId: Long = uniqueId(),
    eventType: String = "view",
    itemId: Option[Long] = Some(uniqueId()),
    properties: Map[String, String] = Map("test" -> "true"),
    timestamp: Long = System.currentTimeMillis(),
    processingTimestamp: Long = System.currentTimeMillis()
  ): ProcessedEvent = {
    ProcessedEvent(
      id = id,
      userId = userId,
      eventType = eventType,
      itemId = itemId,
      properties = properties,
      timestamp = timestamp,
      processingTimestamp = processingTimestamp
    )
  }
  
  // Create test recommendations
  def createRecommendation(
    id: Option[Long] = None,
    userId: Long = uniqueId(), 
    recommendedItems: List[RecommendedItem] = List.empty,
    score: Double = 0.85,
    timestamp: Long = System.currentTimeMillis()
  ): Recommendation = {
    Recommendation(
      id = id,
      userId = userId,
      recommendedItems = if (recommendedItems.isEmpty) 
                           List(createRecommendedItem()) 
                         else 
                           recommendedItems,
      score = score,
      timestamp = timestamp
    )
  }
  
  // Create test recommended items
  def createRecommendedItem(
    itemId: Long = uniqueId(), 
    score: Double = 0.85, 
    reason: String = "test recommendation"
  ): RecommendedItem = {
    RecommendedItem(
      itemId = itemId,
      score = score,
      reason = reason
    )
  }
  
  // Create test user friends
  def createUserFriend(
    userId: Long = uniqueId(),
    friendId: Long = uniqueId(),
    similarity: Double = 0.8
  ): UserFriend = {
    UserFriend(
      userId = userId,
      friendId = friendId,
      similarity = similarity
    )
  }
  
  // Create a complete test dataset
  def createTestDataset(
    userCount: Int = 5,
    eventsPerUser: Int = 3,
    friendsPerUser: Int = 2
  ): TestDataset = {
    // Create users
    val users = (1 to userCount).map(_ => createUser()).toList
    
    // Create events for each user
    val events = users.flatMap { user =>
      (1 to eventsPerUser).map(_ => createEvent(createdBy = user.userId))
    }
    
    // Create user events
    val userEvents = users.flatMap { user =>
      (1 to eventsPerUser).map { i =>
        val eventType = if (i % 3 == 0) "purchase" else if (i % 2 == 0) "click" else "view"
        createUserEvent(userId = user.userId, eventType = eventType, itemId = events.lift(i % events.size).flatMap(_.id))
      }
    }
    
    // Create processed events
    val processedEvents = userEvents.map { event =>
      createProcessedEvent(
        userId = event.userId,
        eventType = event.eventType,
        itemId = event.itemId,
        properties = event.properties.getOrElse(Map.empty)
      )
    }
    
    // Create friendships (each user is friends with a random subset of other users)
    val friendships = users.flatMap { user =>
      val otherUsers = users.filterNot(_.userId == user.userId)
      Random.shuffle(otherUsers).take(friendsPerUser).map { friend =>
        createUserFriend(userId = user.userId, friendId = friend.userId)
      }
    }
    
    TestDataset(
      users = users,
      events = events,
      userEvents = userEvents,
      processedEvents = processedEvents,
      friendships = friendships
    )
  }
  
  // Container for a complete test dataset
  case class TestDataset(
    users: List[User],
    events: List[Event],
    userEvents: List[UserEvent],
    processedEvents: List[ProcessedEvent],
    friendships: List[UserFriend]
  )
}

