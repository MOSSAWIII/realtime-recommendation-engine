package models

import java.time.LocalDateTime
import slick.jdbc.PostgresProfile.api._

// Domain models
case class User(
  userId: Long,
  locale: Option[String] = None,
  birthyear: Option[Int] = None,
  gender: Option[String] = None,
  joinedAt: LocalDateTime,
  location: Option[String] = None,
  timezone: Option[Int] = None,
  isGenerated: Boolean = false
)

case class Event(
  eventId: Long,
  userId: Long, // organizer
  startTime: LocalDateTime,
  city: String,
  state: String,
  country: String,
  lat: Option[Double] = None,
  lng: Option[Double] = None,
  name: String,
  description: String,
  keywordFrequencies: Map[Int, Int] = Map.empty // c_1 through c_100 columns
)

case class UserInteraction(
  userId: Long,
  eventId: Long,
  status: String
)

case class EventParticipation(
  eventId: Long,
  yesList: Option[String] = None,
  maybeList: Option[String] = None,
  invitedList: Option[String] = None,
  noList: Option[String] = None
) {
  // Helper method to get consolidated list of participating users
  def getParticipatingUserIds: Seq[Long] = {
    val yesIds = parseUserIdList(yesList)
    val maybeIds = parseUserIdList(maybeList)
    yesIds ++ maybeIds
  }
  
  private def parseUserIdList(list: Option[String]): Seq[Long] = {
    list.map(_.split(",").filter(_.nonEmpty).map(_.trim.toLong).toSeq).getOrElse(Seq.empty)
  }
}

case class UserFriends(
  userId: Long,
  friendId: Long
)

// Table definitions
object Tables {
  class UsersTable(tag: Tag) extends Table[User](tag, "utilisateur") {
    def userId = column[Long]("user_id", O.PrimaryKey)
    def locale = column[Option[String]]("locale")
    def birthyear = column[Option[Int]]("birthyear")
    def gender = column[Option[String]]("gender")
    def joinedAt = column[LocalDateTime]("joinedat")
    def location = column[Option[String]]("location")
    def timezone = column[Option[Int]]("timezone")
    def isGenerated = column[Boolean]("is_generated")

    def * = (userId, locale, birthyear, gender, joinedAt, location, timezone, isGenerated) <> 
      ((User.apply _).tupled, User.unapply)
  }

  class EventsTable(tag: Tag) extends Table[Event](tag, "evenement") {
    def eventId = column[Long]("event_id", O.PrimaryKey)
    def userId = column[Long]("user_id")
    def startTime = column[LocalDateTime]("start_time")
    def city = column[String]("city")
    def state = column[String]("state")
    def country = column[String]("country")
    def lat = column[Option[Double]]("lat")
    def lng = column[Option[Double]]("lng")
    def name = column[String]("name")
    def description = column[String]("description")
    
    // Define columns c_1 through c_100
    // This avoids manually typing 100 column definitions
    val keywordColumns = (1 to 100).map { i =>
      column[Int](s"c_$i", O.Default(0))
    }
    
    def cOther = column[Int]("c_other", O.Default(0))
    
    // Custom mapping function to convert between columns and Map
    private def columnsToMap(columns: Seq[Int], cOther: Int): Map[Int, Int] = {
      val map = collection.mutable.Map[Int, Int]()
      for (i <- columns.indices) {
        if (columns(i) > 0) {
          map(i + 1) = columns(i)  // +1 because column indices start at 1 (c_1)
        }
      }
      if (cOther > 0) {
        map(0) = cOther  // Use 0 to represent c_other
      }
      map.toMap
    }
    
    private def mapToColumns(map: Map[Int, Int]): (Seq[Int], Int) = {
      val columns = (1 to 100).map(i => map.getOrElse(i, 0))
      val cOther = map.getOrElse(0, 0)
      (columns, cOther)
    }
    
    // The * projection using our custom mapping
    def * = (
      eventId, userId, startTime, city, state, country, lat, lng, name, description, 
      (keywordColumns, cOther).mapTo[Map[Int, Int]](columnsToMap, mapToColumns)
    ) <> ((Event.apply _).tupled, Event.unapply)
  }

  class InteractionsTable(tag: Tag) extends Table[UserInteraction](tag, "participation_detail") {
    def userId = column[Long]("user_id")
    def eventId = column[Long]("event_id")
    def status = column[String]("status")

    def pk = primaryKey("pk_interaction", (userId, eventId))
    def * = (userId, eventId, status) <> 
      ((UserInteraction.apply _).tupled, UserInteraction.unapply)
  }

  class UserFriendsTable(tag: Tag) extends Table[UserFriends](tag, "amitie") {
    def userId = column[Long]("user_id")
    def friendId = column[Long]("friend_id")

    def pk = primaryKey("pk_user_friends", (userId, friendId))
    def * = (userId, friendId) <> ((UserFriends.apply _).tupled, UserFriends.unapply)
  }

  class EventParticipationsTable(tag: Tag) extends Table[EventParticipation](tag, "participation_evenement") {
    def eventId = column[Long]("event_id", O.PrimaryKey)
    def yesList = column[Option[String]]("yes_list")
    def maybeList = column[Option[String]]("maybe_list")
    def invitedList = column[Option[String]]("invited_list")
    def noList = column[Option[String]]("no_list")

    def * = (eventId, yesList, maybeList, invitedList, noList) <> 
      ((EventParticipation.apply _).tupled, EventParticipation.unapply)
  }

  // Table queries
  val users = TableQuery[UsersTable]
  val events = TableQuery[EventsTable]
  val interactions = TableQuery[InteractionsTable]
  val userFriends = TableQuery[UserFriendsTable]
  val eventParticipations = TableQuery[EventParticipationsTable]
}
