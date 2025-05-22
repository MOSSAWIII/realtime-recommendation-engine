package domain.models

case class UserInteraction(
  id: Option[Long],
  userId: Long,
  itemId: Long,
  interactionType: String,
  timestamp: Long,
  metadata: Option[Map[String, String]] = None
)