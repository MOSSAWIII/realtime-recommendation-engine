package domain.models

case class UserEvent(
  userId: Long,
  eventType: String,
  itemId: Option[Long] = None,
  properties: Option[Map[String, String]] = None,  // Changer en Option
  timestamp: Long = System.currentTimeMillis()
)