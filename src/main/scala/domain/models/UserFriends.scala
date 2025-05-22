package domain.models

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

/**
 * Represents a friendship between two users with a strength value.
 *
 * @param userId The ID of the user
 * @param friendId The ID of the friend
 * @param strength The strength of the relationship (0.0 to 1.0)
 */
case class UserFriend(
  userId: Long,
  friendId: Long,
  strength: Double = 1.0
)

object UserFriend {
  implicit val userFriendDecoder: Decoder[UserFriend] = deriveDecoder[UserFriend]
  implicit val userFriendEncoder: Encoder[UserFriend] = deriveEncoder[UserFriend]
}

/**
 * Container for a user's friends list.
 *
 * @param userId The ID of the user
 * @param friends List of user friends
 */
case class UserFriends(
  userId: Long,
  friends: List[UserFriend] = List.empty
)

object UserFriends {
  implicit val userFriendsDecoder: Decoder[UserFriends] = deriveDecoder[UserFriends]
  implicit val userFriendsEncoder: Encoder[UserFriends] = deriveEncoder[UserFriends]
}

