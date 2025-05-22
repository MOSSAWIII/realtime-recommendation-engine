package domain.models

import java.time.LocalDateTime

case class User(
  userId: Long,
  name: Option[String] = None,
  emailAddress: Option[String] = None,
  locale: String = "en_US",
  birthYear: Option[Int] = None,
  gender: Option[String] = None,
  location: Option[String] = None,
  timezone: Option[Int] = None,
  isGenerated: Boolean = false
)