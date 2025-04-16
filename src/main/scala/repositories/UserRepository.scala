package repositories

import models.User
import config.DatabaseConfig
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import io.circe.parser._
import io.circe.syntax._

class UserRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  def findAll(): Future[List[User]] = executeWithErrorHandling({
    Future {
      val users = scala.collection.mutable.ListBuffer[User]()
      
      withConnection { connection =>
        val sql = "SELECT user_id, locale, birthyear, gender, joinedat, location, timezone FROM UTILISATEUR"
        
        withPreparedStatement(connection, sql) { ps =>
          withResultSet(ps) { rs =>
            while (rs.next()) {
              users += User(
                userId = rs.getLong("user_id"),
                locale = Option(rs.getString("locale")),
                birthYear = Option(rs.getObject("birthyear")).map(_.toString.toFloat),
                gender = Option(rs.getString("gender")),
                joinedAt = Option(rs.getTimestamp("joinedat")).map(_.toInstant),
                location = Option(rs.getString("location")),
                timezone = Option(rs.getObject("timezone")).map(_.toString.toInt)
              )
            }
          }
        }
      }
      
      users.toList
    }
  }, "Error fetching all users")

  def upsert(user: User): Future[User] = executeWithErrorHandling({
    Future {
      withConnection { connection =>
        val sql = """
          INSERT INTO UTILISATEUR (user_id, locale, birthyear, gender, joinedat, location, timezone)
          VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (user_id) 
          DO UPDATE SET 
            locale = EXCLUDED.locale,
            birthyear = EXCLUDED.birthyear,
            gender = EXCLUDED.gender,
            joinedat = EXCLUDED.joinedat,
            location = EXCLUDED.location,
            timezone = EXCLUDED.timezone
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, user.userId)
          ps.setObject(2, user.locale.orNull)
          ps.setObject(3, user.birthYear.map(_.asInstanceOf[Object]).orNull)
          ps.setObject(4, user.gender.orNull)
          ps.setObject(5, user.joinedAt.map(java.sql.Timestamp.from).orNull)
          ps.setObject(6, user.location.orNull)
          ps.setObject(7, user.timezone.map(_.asInstanceOf[Object]).orNull)
          
          ps.executeUpdate()
        }
        
        user
      }
    }
  }, s"Error upserting user ${user.userId}")

  private def withConnection[T](operation: Connection => T): T = {
    var connection: Connection = null
    try {
      connection = DatabaseConfig.getConnection()
      operation(connection)
    } finally {
      if (connection != null) {
        try {
          connection.close()
        } catch {
          case e: Exception => logger.warn(s"Failed to close connection: ${e.getMessage}", e)
        }
      }
    }
  }

  private def withPreparedStatement[T](connection: Connection, sql: String)
                                    (operation: PreparedStatement => T): T = {
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      operation(ps)
    } finally {
      if (ps != null) {
        try {
          ps.close()
        } catch {
          case e: Exception => logger.warn(s"Failed to close prepared statement: ${e.getMessage}", e)
        }
      }
    }
  }

  private def withResultSet[T](ps: PreparedStatement)(operation: ResultSet => T): T = {
    var rs: ResultSet = null
    try {
      rs = ps.executeQuery()
      operation(rs)
    } finally {
      if (rs != null) {
        try {
          rs.close()
        } catch {
          case e: Exception => logger.warn(s"Failed to close result set: ${e.getMessage}", e)
        }
      }
    }
  }
}

