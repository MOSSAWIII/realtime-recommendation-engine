package models

/**
 * Message for requesting recommendations internally.
 * Used to decouple the Transformer from the Recommender.
 */
case class RecommendationRequest(
  userId: Long,
  timestamp: Long = System.currentTimeMillis()
)

