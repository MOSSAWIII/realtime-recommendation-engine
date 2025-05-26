package testutil

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import java.sql.{Connection, PreparedStatement, ResultSet}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._  // Add this import for any matcher
import common.mocks.MockManager

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[infrastructure.config.DatabaseConfig.type]))
class BaseRepositorySpec extends AnyFunSuite with BeforeAndAfterEach {

  var mockConnection: Connection = _
  var mockPreparedStatement: PreparedStatement = _
  var mockResultSet: ResultSet = _

  override def beforeEach(): Unit = {
    // Get mock connection from TestImports
    mockConnection = MockManager.mockDatabaseConnection()
    mockPreparedStatement = PowerMockito.mock(classOf[PreparedStatement])
    mockResultSet = PowerMockito.mock(classOf[ResultSet])
    
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
  }
}

