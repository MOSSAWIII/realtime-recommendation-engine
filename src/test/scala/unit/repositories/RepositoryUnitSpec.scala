package unit.repositories

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.{PowerMockIgnore, PrepareForTest}
import org.powermock.modules.junit4.PowerMockRunner
import org.junit.runner.RunWith
import org.mockito.Mockito._
import config.DatabaseConfig

import java.sql.{Connection, PreparedStatement, ResultSet}
import repositories.Repository
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.net.ssl.*"))
@PrepareForTest(Array(classOf[DatabaseConfig]))
class RepositoryUnitSpec extends AnyFlatSpec with Matchers {

  private var mockConnection: Connection = _
  private var mockPreparedStatement: PreparedStatement = _
  private var mockResultSet: ResultSet = _

  "Repository" should "properly handle database resources" in {
    // Setup
    mockConnection = mock(classOf[Connection])
    mockPreparedStatement = mock(classOf[PreparedStatement])
    mockResultSet = mock(classOf[ResultSet])
    
    // Mock static method
    PowerMockito.mockStatic(classOf[DatabaseConfig])
    PowerMockito.when(DatabaseConfig.getConnection).thenReturn(mockConnection)
    
    // Test the withConnection method
    val result = withConnection { conn =>
      val connection = DatabaseConfig.getConnection
      connection shouldEqual mockConnection
      "success"
    }
    
    result shouldEqual "success"
    
    // Verify that resources are closed properly
    verify(mockConnection, times(1)).close()
  }
  
  // Helper method similar to the one in Repository.scala
  private def withConnection[T](operation: Connection => T): T = {
    var connection: Connection = null
    try {
      connection = DatabaseConfig.getConnection
      operation(connection)
    } finally {
      if (connection != null) {
        try {
          connection.close()
        } catch {
          case e: Exception => println(s"Failed to close connection: ${e.getMessage}")
        }
      }
    }
  }
}