package models

import java.time.Instant

case class User(
  userId: Long,
  locale: Option[String] = None,
  birthYear: Option[Float] = None,
  gender: Option[String] = None,
  joinedAt: Option[Instant] = None,
  location: Option[String] = None,
  timezone: Option[Int] = None
)