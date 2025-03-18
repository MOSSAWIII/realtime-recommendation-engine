import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class Event(
  eventId: Int,            // Changé de String à Int
  userId: Int,             // Ajouté
  startTime: LocalDateTime,
  city: Option[String],    // Changé en Option car nullable
  country: Option[String], // Changé en Option car nullable
  lat: Option[Double],     // Ajouté
  lng: Option[Double],     // Ajouté
  keywordFrequencies: Map[Int, Int], // Pour stocker c_1 à c_100
  otherKeywordsFrequency: Int        // Pour c_other
)

case class User(
  userId: Int,             // Changé de String à Int
  locale: String,
  birthYear: Double,
  gender: String,
  joinedAt: LocalDateTime,
  location: Option[String],
  timezone: Int
)

case class UserParticipation(
  eventId: Int,
  yesUsers: Set[Int],
  maybeUsers: Set[Int],
  invitedUsers: Set[Int],
  noUsers: Set[Int]
)

case class UserFriends(
  userId: Int,
  friendsList: Set[Int]
)

// Mise à jour de UserInteraction
case class UserInteraction(
  userId: Int,            // Changé de String à Int
  searchQuery: Option[String],
  clickedEventIds: List[Int],  // Changé de String à Int
  currentLocation: Option[String],
  timeRange: Option[(LocalDateTime, LocalDateTime)]
)