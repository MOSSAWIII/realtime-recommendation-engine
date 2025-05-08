package unit.repositories

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.junit.runner.RunWith
import repositories.FriendsRepository
import models.{UserFriends, UserFriend}
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.ExecutionContext.Implicits.global
import config._
import scala.concurrent.Future

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[config.DatabaseConfig.type]))
class FriendsRepositoryBaseSpec extends AnyFlatSpec with Matchers {
  // Setup mock
  val mockConnection = PowerMockito.mock(classOf[Connection])
  PowerMockito.mockStatic(classOf[config.DatabaseConfig.type])
  when(config.DatabaseConfig.getConnection).thenReturn(mockConnection)
}

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[config.DatabaseConfig.type]))
class FriendsRepositorySpec extends FriendsRepositoryBaseSpec {

  "FriendsRepository" should "attempt to upsert user friends" in {
    // Arrange
    val mockPreparedStatement = PowerMockito.mock(classOf[PreparedStatement])
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeUpdate()).thenReturn(1)

    val friendsRepository = new FriendsRepository()
    // Update to use the new UserFriends structure with friends list
    val userFriendsList = List(UserFriend(1L, 2L, 1.0), UserFriend(1L, 3L, 1.0))
    val userFriends = UserFriends(userId = 1L, friends = userFriendsList)
    val expectedFriendsList = "2 3"  // Expected string representation of friendIds

    // Act
    friendsRepository.upsert(userFriends)

    // Assert
    verify(mockConnection, timeout(1000)).prepareStatement(contains("INSERT INTO"))
    verify(mockPreparedStatement, timeout(1000)).setLong(1, userFriends.userId)
    verify(mockPreparedStatement, timeout(1000)).setString(2, expectedFriendsList)
    verify(mockPreparedStatement, timeout(1000)).executeUpdate()
    verify(mockPreparedStatement, timeout(1000)).close()
    verify(mockConnection, timeout(1000)).close()
  }

  "FriendsRepository" should "attempt to find friends by user ID" in {
    // Arrange
    val mockPreparedStatement = PowerMockito.mock(classOf[PreparedStatement])
    val mockResultSet = PowerMockito.mock(classOf[ResultSet])
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
    when(mockResultSet.next()).thenReturn(true, false)
    when(mockResultSet.getLong("user_id")).thenReturn(1L)
    when(mockResultSet.getString("friends_list")).thenReturn("2 3")  // Changed from friend_ids to friends_list

    val friendsRepository = new FriendsRepository()
    val userId = 1L

    // Act
    friendsRepository.findFriendsByUserId(userId.toInt)

    // Assert
    verify(mockConnection, timeout(1000)).prepareStatement(contains("SELECT user_id, friends_list FROM"))
    verify(mockPreparedStatement, timeout(1000)).setLong(1, userId)
    verify(mockPreparedStatement, timeout(1000)).executeQuery()
    verify(mockResultSet, timeout(1000)).next()
    verify(mockResultSet, timeout(1000)).getLong("user_id")
    verify(mockResultSet, timeout(1000)).getString("friends_list")
    verify(mockResultSet, timeout(1000)).close()
    verify(mockPreparedStatement, timeout(1000)).close()
    verify(mockConnection, timeout(1000)).close()
  }

  def findFriendsByUserId(userId: Long): Future[Option[UserFriends]] = 
    findByUserId(userId)

  def findByUserId(userId: Long): Future[Option[UserFriends]] = {
    findFriendsByUserId(userId)
  }
}