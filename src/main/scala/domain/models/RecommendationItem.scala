package domain.models

/**
 * Représente un élément de recommandation pour un utilisateur
 */
case class RecommendationItem(
  itemId: Long,
  score: Double,
  reason: String,
  itemType: String
)

