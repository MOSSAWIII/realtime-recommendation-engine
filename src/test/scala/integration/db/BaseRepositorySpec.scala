package testutil

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import java.sql.{Connection, PreparedStatement, ResultSet}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._  // Add this import for any matcher
import common.mocks.MockManager

class BaseRepositorySpec extends AnyFunSuite with BeforeAndAfterEach {

  var mockConnection: Connection = _
  var mockPreparedStatement: PreparedStatement = _
  var mockResultSet: ResultSet = _

  override def beforeEach(): Unit = {
    // Get mock connection from TestImports
    mockConnection = MockManager.mockDatabaseConnection()
    mockPreparedStatement = mock(classOf[PreparedStatement])
    mockResultSet = mock(classOf[ResultSet])
    
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
  }
}

