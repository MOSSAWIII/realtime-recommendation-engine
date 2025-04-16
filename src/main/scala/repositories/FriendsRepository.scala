package repositories

import models.UserFriends
import config.DatabaseConfig
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class FriendsRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  def findAll(): Future[List[UserFriends]] = executeWithErrorHandling({
    Future {
      val userFriends = scala.collection.mutable.ListBuffer[UserFriends]()
      
      withConnection { connection =>
        val sql = """
          SELECT user_id, friends_list 
          FROM RESEAU_AMIS
        """
        
        withPreparedStatement(connection, sql) { ps =>
          withResultSet(ps) { rs =>
            while (rs.next()) {
              userFriends += mapResultSetToUserFriends(rs)
            }
          }
        }
      }
      
      userFriends.toList
    }
  }, "Error fetching all user friendships")

  def findByUserId(userId: Long): Future[Option[UserFriends]] = executeWithErrorHandling({
    Future {
      var userFriend: Option[UserFriends] = None
      
      withConnection { connection =>
        val sql = """
          SELECT user_id, friends_list 
          FROM RESEAU_AMIS
          WHERE user_id = ?
        """
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, userId)
          
          withResultSet(ps) { rs =>
            if (rs.next()) {
              userFriend = Some(mapResultSetToUserFriends(rs))
            }
          }
        }
      }
      
      userFriend
    }
  }, s"Error fetching friendships for user $userId")
  
  def upsert(userFriends: UserFriends): Future[UserFriends] = executeWithErrorHandling({
    Future {
      withConnection { connection =>
        val sql = """
          INSERT INTO RESEAU_AMIS (user_id, friends_list)
          VALUES (?, ?)
          ON CONFLICT (user_id) 
          DO UPDATE SET 
            friends_list = EXCLUDED.friends_list
        """
        
        val friendsList = userFriends.friendIds.mkString(" ")
        
        withPreparedStatement(connection, sql) { ps =>
          ps.setLong(1, userFriends.userId)
          ps.setString(2, friendsList)
          
          ps.executeUpdate()
        }
        
        userFriends
      }
    }
  }, s"Error upserting friendships for user ${userFriends.userId}")
  
  private def mapResultSetToUserFriends(rs: ResultSet): UserFriends = {
    val userId = rs.getLong("user_id")
    val friendsListStr = rs.getString("friends_list")
    
    val friendIds = if (friendsListStr != null && friendsListStr.nonEmpty) {
      friendsListStr.split(" ").flatMap(id => 
        Try(id.trim.toLong).toOption
      ).toList
    } else {
      List.empty[Long]
    }
    
    UserFriends(
      userId = userId,
      friendIds = friendIds
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

