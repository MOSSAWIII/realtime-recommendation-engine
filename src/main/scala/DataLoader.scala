import com.github.tototoshi.csv._
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.json4s._
import org.json4s.native.JsonMethods._
import scala.io.Source

object DataLoader {
  // Format de date pour les événements
  private val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  
  // Chargement de la structure de la base depuis le fichier JSON
  def loadDatabaseSchema(): JValue = {
    val jsonFile = Source.fromFile("docs/database.json")
    try {
      parse(jsonFile.mkString)
    } finally {
      jsonFile.close()
    }
  }
  
    def loadEvents(): List[Event] = {
    val reader = CSVReader.open(new File("database/events.csv"))
    try {
        val rows = reader.allWithHeaders()
        rows.map { row =>
        val keywordFreqs = (1 to 100).map { i =>
            i -> row.getOrElse(s"c_$i", "0").toInt
        }.filter(_._2 > 0).toMap

        Event(
            eventId = row("event_id").toInt,
            userId = row("user_id").toInt,
            startTime = LocalDateTime.parse(row("start_time"), dateFormat),
            city = Option(row("city")).filter(_.nonEmpty),
            country = Option(row("country")).filter(_.nonEmpty),
            lat = Option(row("lat")).map(_.toDouble),
            lng = Option(row("lng")).map(_.toDouble),
            keywordFrequencies = keywordFreqs,
            otherKeywordsFrequency = row.getOrElse("c_other", "0").toInt
        )
        }
    } finally {
        reader.close()
    }
    }

    def loadUsers(): List[User] = {
    val reader = CSVReader.open(new File("database/users.csv"))
    try {
        val rows = reader.allWithHeaders()
        rows.map { row =>
        User(
            userId = row("user_id").toInt,
            locale = row("locale"),
            birthYear = row("birthyear").toDouble,
            gender = row("gender"),
            joinedAt = LocalDateTime.parse(row("joinedAt"), dateFormat),
            location = Option(row("location")).filter(_.nonEmpty),
            timezone = row("timezone").toInt
        )
        }
    } finally {
        reader.close()
    }
    }
  
  private def loadAttendeeCountForEvent(eventId: String): Int = {
    val reader = CSVReader.open(new File("database/event_attendees.csv"))
    try {
      reader.all().count(row => row.head == eventId)
    } finally {
      reader.close()
    }
  }
  
  private def loadFriendsForUser(userId: String): List[String] = {
    val reader = CSVReader.open(new File("database/user_friends.csv"))
    try {
      reader.all().filter(row => row.head == userId).map(_(1)).toList
    } finally {
      reader.close()
    }
  }
}