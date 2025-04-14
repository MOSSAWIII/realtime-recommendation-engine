package models

// Modèle d'événement d'entrée
case class UserEvent(
  userId: Long,
  eventType: String,
  itemId: Option[Long] = None,
  properties: Map[String, String] = Map.empty,
  timestamp: Long = System.currentTimeMillis()
)

// Modèle d'événement transformé et stocké
case class ProcessedEvent(
  id: Option[Long] = None,
  userId: Long,
  eventType: String,
  itemId: Option[Long],
  properties: Map[String, String],
  timestamp: Long,
  // Métadonnées additionnelles après transformation
  processingTimestamp: Long = System.currentTimeMillis()
)

// Modèle de recommandation (sortie)
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