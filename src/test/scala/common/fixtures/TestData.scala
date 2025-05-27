package common.fixtures

import domain.models._
import java.util.UUID

object TestData {
  def createTestUserEvent(userId: Long = 100L): UserEvent = {
    UserEvent(
      userId = userId,
      eventType = "view",
      itemId = Some(200L),
      properties = Some(Map("test" -> "true")),
      timestamp = System.currentTimeMillis()
    )
  }
  
  def createTestUser(userId: Long = 100L): User = {
    User(
      userId = userId,
      name = Some(s"User $userId"),
      emailAddress = Some(s"user$userId@example.com"),
      locale = "en_US",
      birthYear = Some(1990),
      gender = Some("M"),
      location = Some("Test Location"),
      timezone = Some(1),
      isGenerated = false
    )
  }
  
  def createTestRecommendationRequest(userId: Long = 100L): RecommendationRequest = {
    RecommendationRequest(userId)
  }
  
  def createUniqueQueueName(): String = {
    s"test_queue_${UUID.randomUUID().toString.take(8)}"
  }
}

