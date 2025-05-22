package domain.models

case class Friendship(
  id: Option[Long],
  userId1: Long,
  userId2: Long,
  status: String,
  createdAt: Long
)