package repositories

import models.{Friendship, UserFriends, UserFriend}
import config.DatabaseConfig
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import org.slf4j.LoggerFactory

class FriendsRepository(implicit override val ec: ExecutionContext) extends Repository {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def upsert(friendship: Friendship): Future[Int] = {
    // In real implementation, this would insert or update a friendship in the database
    Future.successful(1)
  }

  def findAll(): Future[List[UserFriends]] = executeWithErrorHandling({
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
  }, "Error fetching all user friendships")

  def findByUserId(userId: Long): Future[Option[UserFriends]] = executeWithErrorHandling({
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
  }, s"Error fetching friendships for user $userId")
  
  def upsert(userFriends: UserFriends): Future[UserFriends] = executeWithErrorHandling({
    withConnection { connection =>
      val sql = """
        INSERT INTO RESEAU_AMIS (user_id, friends_list)
        VALUES (?, ?)
        ON CONFLICT (user_id) 
        DO UPDATE SET 
          friends_list = EXCLUDED.friends_list
      """
      
      // Extract just the friend IDs for storage
      val friendsList = userFriends.friends.map(_.friendId).mkString(" ")
      
      withPreparedStatement(connection, sql) { ps =>
        ps.setLong(1, userFriends.userId)
        ps.setString(2, friendsList)
        
        ps.executeUpdate()
      }
      
      userFriends
    }
  }, s"Error upserting friendships for user ${userFriends.userId}")
  
  def findFriendsByUserId(userId: Int): List[Int] = {
    // Implementation details
    // This is a placeholder - implement according to your business logic
    List.empty[Int]
  }

  private def mapResultSetToUserFriends(rs: ResultSet): UserFriends = {
    val userIdValue = rs.getLong("user_id")
    val friendsListStr = rs.getString("friends_list")
    
    val friendsList = if (friendsListStr != null && friendsListStr.nonEmpty) {
      friendsListStr.split(" ").flatMap(id => 
        Try(id.trim.toLong).toOption
      ).map(friendId => UserFriend(userIdValue, friendId, 1.0)).toList
    } else {
      List.empty[UserFriend]
    }
    
    UserFriends(
      userId = userIdValue,
      friends = friendsList
    )
  }
}

