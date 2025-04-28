package models

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

case class UserFriends(
  userId: Long,
  friendIds: List[Long]
)

object UserFriends {
  implicit val userFriendsDecoder: Decoder[UserFriends] = deriveDecoder[UserFriends]
  implicit val userFriendsEncoder: Encoder[UserFriends] = deriveEncoder[UserFriends]
}

