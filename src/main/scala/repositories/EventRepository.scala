package repositories

import models.Event
import config.DatabaseConfig
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import java.time.{LocalDateTime, Instant}
import scala.collection.mutable.Map

class EventRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  def findById(eventId: Long): Option[Event] = {
    var event: Option[Event] = None
    
    withConnection { connection =>
      val sql = """
        SELECT event_id, user_id, start_time, city, state, country, 
               lat, lng, c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8, c_9, c_10,
               c_11, c_12, c_13, c_14, c_15, c_16, c_17, c_18, c_19, c_20,
               c_21, c_22, c_23, c_24, c_25, c_26, c_27, c_28, c_29, c_30,
               c_31, c_32, c_33, c_34, c_35, c_36, c_37, c_38, c_39, c_40,
               c_41, c_42, c_43, c_44, c_45, c_46, c_47, c_48, c_49, c_50,
               c_51, c_52, c_53, c_54, c_55, c_56, c_57, c_58, c_59, c_60,
               c_61, c_62, c_63, c_64, c_65, c_66, c_67, c_68, c_69, c_70,
               c_71, c_72, c_73, c_74, c_75, c_76, c_77, c_78, c_79, c_80,
               c_81, c_82, c_83, c_84, c_85, c_86, c_87, c_88, c_89, c_90,
               c_91, c_92, c_93, c_94, c_95, c_96, c_97, c_98, c_99, c_100
        FROM EVENEMENT 
        WHERE event_id = ?
      """
      
      withPreparedStatement(connection, sql) { ps =>
        ps.setLong(1, eventId)
        
        withResultSet(ps) { rs =>
          if (rs.next()) {
            event = Some(mapResultSetToEvent(rs))
          }
        }
      }
    }
    
    event
  }
  
  def findAll(): Future[List[Event]] = executeWithErrorHandling({
    Future {
      val events = scala.collection.mutable.ListBuffer[Event]()
      
      withConnection { connection =>
        val sql = """
          SELECT event_id, user_id, start_time, city, state, country, 
                 lat, lng 
          FROM EVENEMENT
          LIMIT 1000
        """
        
        withPreparedStatement(connection, sql) { ps =>
          withResultSet(ps) { rs =>
            while (rs.next()) {
              events += Event(
                eventId = rs.getLong("event_id"),
                userId = rs.getLong("user_id"),
                title = "Event " + rs.getLong("event_id"), // Default title as it's missing in the schema
                description = None,
                startTime = Option(rs.getTimestamp("start_time"))
                  .map(ts => ts.toLocalDateTime),
                city = Option(rs.getString("city")),
                state = Option(rs.getString("state")),
                country = Option(rs.getString("country")),
                latitude = Option(rs.getObject("lat"))
                  .map(_.toString.toDouble),
                longitude = Option(rs.getObject("lng"))
                  .map(_.toString.toDouble),
                keywordFrequencies = Map.empty[Int, Int].toMap
              )
            }
          }
        }
      }
      
      events.toList
    }
  }, "Error fetching all events")
  
  def findFutureEvents(): Future[List[Event]] = executeWithErrorHandling({
    Future {
      val events = scala.collection.mutable.ListBuffer[Event]()
      
      withConnection { connection =>
        val sql = """
          SELECT event_id, user_id, start_time, city, state, country, 
                 lat, lng, c_1, c_2, c_3, c_4, c_5, c_6, c_7, c_8, c_9, c_10,
                 c_11, c_12, c_13, c_14, c_15, c_16, c_17, c_18, c_19, c_20,
                 c_21, c_22, c_23, c_24, c_25, c_26, c_27, c_28, c_29, c_30,
                 c_31, c_32, c_33, c_34, c_35, c_36, c_37, c_38, c_39, c_40,
                 c_41, c_42, c_43, c_44, c_45, c_46, c_47, c_48, c_49, c_50,
                 c_51, c_52, c_53, c_54, c_55, c_56, c_57, c_58, c_59, c_60,
                 c_61, c_62, c_63, c_64, c_65, c_66, c_67, c_68, c_69, c_70,
                 c_71, c_72, c_73, c_74, c_75, c_76, c_77, c_78, c_79, c_80,
                 c_81, c_82, c_83, c_84, c_85, c_86, c_87, c_88, c_89, c_90,
                 c_91, c_92, c_93, c_94, c_95, c_96, c_97, c_98, c_99, c_100
          FROM EVENEMENT 
          WHERE start_time > NOW()
          ORDER BY start_time
          LIMIT 100
        """
        
        withPreparedStatement(connection, sql) { ps =>
          withResultSet(ps) { rs =>
            while (rs.next()) {
              events += mapResultSetToEvent(rs)
            }
          }
        }
      }
      
      events.toList
    }
  }, "Error fetching future events")
  
  def upsert(event: Event): Future[Event] = executeWithErrorHandling({
    Future {
      withConnection { connection =>
        val sql = """
          INSERT INTO EVENEMENT (
            event_id, user_id, start_time, city, state, country, lat, lng
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (event_id) 
          DO UPDATE SET 
            user_id = EXCLUDED.user_id,
            start_time = EXCLUDED.start_time,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            lat = EXCLUDED.lat,
            lng = EXCLUDED.lng
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, event.eventId)
          ps.setLong(2, event.userId)
          ps.setObject(3, event.startTime.orNull)
          ps.setObject(4, event.city.orNull)
          ps.setObject(5, event.state.orNull)
          ps.setObject(6, event.country.orNull)
          ps.setObject(7, event.latitude.map(_.asInstanceOf[Object]).orNull)
          ps.setObject(8, event.longitude.map(_.asInstanceOf[Object]).orNull)
          
          ps.executeUpdate()
        }
        
        event
      }
    }
  }, s"Error upserting event ${event.eventId}")
  
  private def mapResultSetToEvent(rs: ResultSet): Event = {
    // Extract keyword frequencies from c_1 to c_100
    val keywordFrequencies = scala.collection.mutable.Map[Int, Int]()
    for (i <- 1 to 100) {
      val value = rs.getObject(s"c_$i")
      if (value != null) {
        val freq = value.toString.toInt
        if (freq > 0) {
          keywordFrequencies(i) = freq
        }
      }
    }
    
    Event(
      eventId = rs.getLong("event_id"),
      userId = rs.getLong("user_id"),
      title = "Event " + rs.getLong("event_id"), // Default title
      description = None,
      startTime = Option(rs.getTimestamp("start_time"))
        .map(ts => ts.toLocalDateTime),
      city = Option(rs.getString("city")),
      state = Option(rs.getString("state")),
      country = Option(rs.getString("country")),
      latitude = Option(rs.getObject("lat"))
        .map(_.toString.toDouble),
      longitude = Option(rs.getObject("lng"))
        .map(_.toString.toDouble),
      keywordFrequencies = keywordFrequencies.toMap
    )
  }
  
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

