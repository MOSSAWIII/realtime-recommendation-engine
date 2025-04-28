package models

case class ProcessedEvent(
  id: Option[Long] = None,
  userId: Long,
  eventType: String,
  itemId: Option[Long],
  properties: Map[String, String] = Map.empty,
  timestamp: Long,
  processingTimestamp: Long = System.currentTimeMillis()
)