package unit.repositories

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach // Add this import
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.modules.junit4.PowerMockRunner
import org.junit.runner.RunWith
import java.sql.{Connection, PreparedStatement, ResultSet}
import config.DatabaseConfig // Ensure correct import

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[DatabaseConfig.type])) // Correct way to mock a Scala object
class RepositoryUnitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  
  var mockConnection: Connection = _
  var mockPreparedStatement: PreparedStatement = _
  var mockResultSet: ResultSet = _
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    mockConnection = PowerMockito.mock(classOf[Connection])
    mockPreparedStatement = PowerMockito.mock(classOf[PreparedStatement])
    mockResultSet = PowerMockito.mock(classOf[ResultSet])
    
    PowerMockito.mockStatic(classOf[DatabaseConfig.type])
    PowerMockito.when(DatabaseConfig.getConnection).thenReturn(mockConnection)
    
    // Setup common mock behavior
    PowerMockito.when(mockConnection.prepareStatement(org.mockito.ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)
    PowerMockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
  }
  
  "Repository" should "get database connection from DatabaseConfig" in {
    val connection = DatabaseConfig.getConnection
    connection should be(mockConnection)
  }
  
  it should "execute queries on the connection" in {
    // Test implementation with your repository class
    val connection = DatabaseConfig.getConnection
    // Actual test code...
  }
}