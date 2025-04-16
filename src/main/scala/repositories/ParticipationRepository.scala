package repositories

import models.EventParticipation
import config.DatabaseConfig
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}

class ParticipationRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  def findAll(): Future[List[EventParticipation]] = executeWithErrorHandling({
    Future {
      val participations = scala.collection.mutable.ListBuffer[EventParticipation]()
      
      withConnection { connection =>
        val sql = """
          SELECT event_id, yes_list, maybe_list, invited_list, no_list 
          FROM PARTICIPATION_EVENEMENT
        """
        
        withPreparedStatement(connection, sql) { ps =>
          withResultSet(ps) { rs =>
            while (rs.next()) {
              participations += EventParticipation(
                eventId = rs.getLong("event_id"),
                yesList = Option(rs.getString("yes_list")),
                maybeList = Option(rs.getString("maybe_list")),
                invitedList = Option(rs.getString("invited_list")),
                noList = Option(rs.getString("no_list"))
              )
            }
          }
        }
      }
      
      participations.toList
    }
  }, "Error fetching all event participations")

  def findByEventId(eventId: Long): Future[Option[EventParticipation]] = executeWithErrorHandling({
    Future {
      var participation: Option[EventParticipation] = None
      
      withConnection { connection =>
        val sql = """
          SELECT event_id, yes_list, maybe_list, invited_list, no_list 
          FROM PARTICIPATION_EVENEMENT
          WHERE event_id = ?
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, eventId)
          
          withResultSet(ps) { rs =>
            if (rs.next()) {
              participation = Some(EventParticipation(
                eventId = rs.getLong("event_id"),
                yesList = Option(rs.getString("yes_list")),
                maybeList = Option(rs.getString("maybe_list")),
                invitedList = Option(rs.getString("invited_list")),
                noList = Option(rs.getString("no_list"))
              ))
            }
          }
        }
      }
      
      participation
    }
  }, s"Error fetching participation for event $eventId")
  
  def upsert(participation: EventParticipation): Future[EventParticipation] = executeWithErrorHandling({
    Future {
      withConnection { connection =>
        val sql = """
          INSERT INTO PARTICIPATION_EVENEMENT (event_id, yes_list, maybe_list, invited_list, no_list)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT (event_id) 
          DO UPDATE SET 
            yes_list = EXCLUDED.yes_list,
            maybe_list = EXCLUDED.maybe_list,
            invited_list = EXCLUDED.invited_list,
            no_list = EXCLUDED.no_list
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, participation.eventId)
          ps.setObject(2, participation.yesList.orNull)
          ps.setObject(3, participation.maybeList.orNull)
          ps.setObject(4, participation.invitedList.orNull)
          ps.setObject(5, participation.noList.orNull)
          
          ps.executeUpdate()
        }
        
        participation
      }
    }
  }, s"Error upserting participation for event ${participation.eventId}")
  
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

