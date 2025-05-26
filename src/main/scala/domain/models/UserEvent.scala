package domain.models

case class UserEvent(
  userId: Long,
  eventType: String,
  itemId: Option[Long] = None,
  properties: Option[Map[String, String]] = None,  // Changer en Option
  timestamp: Long = System.currentTimeMillis()
)

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

object UserEvent {
  implicit val decoder: Decoder[UserEvent] = deriveDecoder[UserEvent]
  implicit val encoder: Encoder[UserEvent] = deriveEncoder[UserEvent]
}