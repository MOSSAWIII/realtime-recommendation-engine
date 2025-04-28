package models

case class RecommendationRequest(
  userId: Long,
  timestamp: Long = System.currentTimeMillis()
)

case class Recommendation(
  id: Option[Long] = None,
  userId: Long,
  recommendedItems: List[RecommendedItem],
  score: Double,
  timestamp: Long = System.currentTimeMillis()
)

case class RecommendedItem(
  itemId: Long,
  score: Double,
  reason: String
)

