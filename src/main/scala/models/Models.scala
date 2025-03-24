package models

import slick.jdbc.PostgresProfile.api._
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Case classes pour les entités
case class Utilisateur(
  userId: Long,
  locale: Option[String],
  birthyear: Option[Float],
  gender: Option[String],
  joinedAt: Option[Timestamp],
  location: Option[String],
  timezone: Option[Int]
)

case class Evenement(
  eventId: Long,
  userId: Long,
  startTime: Option[Timestamp],
  city: Option[String],
  state: Option[String],
  zip: Option[String],
  country: Option[String],
  lat: Option[BigDecimal],
  lng: Option[BigDecimal],
  c1: Option[Int],
  c2to99: Option[Int],
  c100: Option[Int],
  cOther: Option[Int]
)

case class ReseauAmis(
  userId: Long,
  friendsList: Option[String]
)

case class ParticipationEvenement(
  eventId: Long,
  yesList: Option[String],
  maybeList: Option[String],
  invitedList: Option[String],
  noList: Option[String]
)

// Classes de table Slick
class UtilisateursTable(tag: Tag) extends Table[Utilisateur](tag, "utilisateur") {
  def userId = column[Long]("user_id", O.PrimaryKey)
  def locale = column[Option[String]]("locale")
  def birthyear = column[Option[Float]]("birthyear")
  def gender = column[Option[String]]("gender")
  def joinedAt = column[Option[Timestamp]]("joinedat")
  def location = column[Option[String]]("location")
  def timezone = column[Option[Int]]("timezone")
  
  def * = (userId, locale, birthyear, gender, joinedAt, location, timezone).mapTo[Utilisateur]
}

// Définir les autres tables similairement...

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