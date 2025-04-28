package models

case class EventParticipation(
  id: Option[Long],
  userId: Long,
  eventId: Long,
  status: String
)