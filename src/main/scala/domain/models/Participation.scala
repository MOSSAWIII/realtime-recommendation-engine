package domain.models

case class Participation(
  userId: Long,
  eventId: Long,
  status: String
)

