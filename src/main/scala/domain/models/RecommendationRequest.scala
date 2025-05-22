package domain.models

/**
 * Request for generating recommendations for a user
 *
 * @param userId ID of the user to generate recommendations for
 * @param contextItemId Optional item ID that provides context (e.g., item being viewed)
 * @param timestamp When the request was made
 */
case class RecommendationRequest(
  userId: Long,
  contextItemId: Option[Long] = None,
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

