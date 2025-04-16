package models

import java.time.{LocalDateTime, Instant}

case class Event(
  eventId: Long,
  userId: Long,
  title: String,
  description: Option[String] = None,
  startTime: Option[LocalDateTime] = None,
  city: Option[String] = None,
  state: Option[String] = None,
  country: Option[String] = None,
  latitude: Option[Double] = None,
  longitude: Option[Double] = None,
  keywordFrequencies: Map[Int, Int] = Map.empty  // From c_1 to c_100
)

case class EventParticipation(
  eventId: Long,
  yesList: Option[String] = None,
  maybeList: Option[String] = None,
  invitedList: Option[String] = None,
  noList: Option[String] = None
)

case class UserFriends(
  userId: Long,
  friendIds: List[Long]
)

