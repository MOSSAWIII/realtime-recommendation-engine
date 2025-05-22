package infrastructure.messaging

import io.circe.{Json, JsonObject, Encoder, Decoder}
import io.circe.parser._
import io.circe.syntax._
import infrastructure.messaging.rabbitmq.RabbitMQManager
import org.slf4j.LoggerFactory

/**
 * Manages message schemas and validation for the recommendation engine.
 */
object MessageSchemaManager {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Types de messages alignés avec l'architecture simplifiée
  val USER_EVENT_TYPE = "user.event"
  val PROCESSED_EVENT_TYPE = "event.processed"
  val RECOMMENDATION_REQUEST_TYPE = "recommendation.request"
  val RECOMMENDATION_RESULT_TYPE = "recommendation.result"
  val HISTORICAL_DATA_TYPE = "historical.data"
  
  /**
   * Initialize schema registry with all message type schemas
   */
  def registerAllSchemas(): Unit = {
    logger.info("Registering message schemas")
    
    // Register user event schema
    val userEventSchema = Json.obj(
      "type" -> Json.fromString("object"),
      "required" -> Json.arr(
        Json.fromString("userId"),
        Json.fromString("eventType")
      ),
      "properties" -> Json.obj(
        "userId" -> Json.obj("type" -> Json.fromString("integer")),
        "eventType" -> Json.obj("type" -> Json.fromString("string")),
        "itemId" -> Json.obj("type" -> Json.fromString("integer")),
        "properties" -> Json.obj("type" -> Json.fromString("object")),
        "timestamp" -> Json.obj("type" -> Json.fromString("integer"))
      )
    )
    RabbitMQManager.registerSchema(USER_EVENT_TYPE, userEventSchema)
    
    // Register processed event schema
    val processedEventSchema = Json.obj(
      "type" -> Json.fromString("object"),
      "required" -> Json.arr(
        Json.fromString("userId"),
        Json.fromString("eventType"),
        Json.fromString("processingTimestamp")
      ),
      "properties" -> Json.obj(
        "id" -> Json.obj("type" -> Json.fromString("integer")),
        "userId" -> Json.obj("type" -> Json.fromString("integer")),
        "eventType" -> Json.obj("type" -> Json.fromString("string")),
        "itemId" -> Json.obj("type" -> Json.fromString("integer")),
        "properties" -> Json.obj("type" -> Json.fromString("object")),
        "timestamp" -> Json.obj("type" -> Json.fromString("integer")),
        "processingTimestamp" -> Json.obj("type" -> Json.fromString("integer"))
      )
    )
    RabbitMQManager.registerSchema(PROCESSED_EVENT_TYPE, processedEventSchema)
    
    // Register recommendation request schema
    val recommendationRequestSchema = Json.obj(
      "type" -> Json.fromString("object"),
      "required" -> Json.arr(
        Json.fromString("userId"),
        Json.fromString("timestamp")
      ),
      "properties" -> Json.obj(
        "userId" -> Json.obj("type" -> Json.fromString("integer")),
        "timestamp" -> Json.obj("type" -> Json.fromString("integer")),
        "contextItems" -> Json.obj(
          "type" -> Json.fromString("array"),
          "items" -> Json.obj("type" -> Json.fromString("integer"))
        ),
        "limit" -> Json.obj("type" -> Json.fromString("integer"))
      )
    )
    RabbitMQManager.registerSchema(RECOMMENDATION_REQUEST_TYPE, recommendationRequestSchema)
    
    // Register recommendation result schema
    val recommendationResultSchema = Json.obj(
      "type" -> Json.fromString("object"),
      "required" -> Json.arr(
        Json.fromString("userId"),
        Json.fromString("recommendations"),
        Json.fromString("timestamp")
      ),
      "properties" -> Json.obj(
        "userId" -> Json.obj("type" -> Json.fromString("integer")),
        "recommendations" -> Json.obj(
          "type" -> Json.fromString("array"),
          "items" -> Json.obj(
            "type" -> Json.fromString("object"),
            "properties" -> Json.obj(
              "itemId" -> Json.obj("type" -> Json.fromString("integer")),
              "score" -> Json.obj("type" -> Json.fromString("number")),
              "reason" -> Json.obj("type" -> Json.fromString("string"))
            )
          )
        ),
        "timestamp" -> Json.obj("type" -> Json.fromString("integer")),
        "generationTime" -> Json.obj("type" -> Json.fromString("integer"))
      )
    )
    RabbitMQManager.registerSchema(RECOMMENDATION_RESULT_TYPE, recommendationResultSchema)
    
    // Register user profile schema
    val userSchema = Json.obj(
      "type" -> Json.fromString("object"),
      "required" -> Json.arr(
        Json.fromString("userId"),
        Json.fromString("name")
      ),
      "properties" -> Json.obj(
        "userId" -> Json.obj("type" -> Json.fromString("integer")),
        "name" -> Json.obj("type" -> Json.fromString("string")),
        "email" -> Json.obj("type" -> Json.fromString("string")),
        "preferences" -> Json.obj("type" -> Json.fromString("object"))
      )
    )
    RabbitMQManager.registerSchema(USER_PROFILE_TYPE, userSchema)
    
    logger.info("All message schemas registered successfully")
  }
  
  /**
   * Get the appropriate routing key for a message based on its type and content
   */
  def getRoutingKeyForMessage[T](messageType: String, message: T): String = {
    messageType match {
      case USER_EVENT_TYPE =>
        val event = message.asInstanceOf[models.UserEvent]
        s"event.raw.${event.eventType}.${event.userId}"
        
      case PROCESSED_EVENT_TYPE =>
        val event = message.asInstanceOf[models.ProcessedEvent]
        s"event.processed.${event.eventType}.${event.userId}"
        
      case USER_PROFILE_TYPE =>
        val user = message.asInstanceOf[models.User]
        s"reference.user.${user.userId}"
        
      case RECOMMENDATION_REQUEST_TYPE =>
        val request = message.asInstanceOf[models.RecommendationRequest]
        s"recommendation.request.${request.userId}"
        
      case RECOMMENDATION_RESULT_TYPE =>
        val result = message.asInstanceOf[models.Recommendation]
        s"recommendation.result.${result.userId}"
        
      case _ =>
        // Default pattern for other types
        s"$messageType.default"
    }
  }
  
  /**
   * Calculate message priority based on content
   */
  def calculatePriority[T](messageType: String, message: T): Option[Int] = {
    messageType match {
      case USER_EVENT_TYPE =>
        val event = message.asInstanceOf[models.UserEvent]
        // Higher priority for purchase events
        if (event.eventType == "purchase") Some(8)
        else if (event.eventType == "click") Some(5)
        else Some(3)
        
      case RECOMMENDATION_REQUEST_TYPE =>
        Some(7) // High priority for recommendation requests
        
      case _ =>
        None // Default priority
    }
  }
}

