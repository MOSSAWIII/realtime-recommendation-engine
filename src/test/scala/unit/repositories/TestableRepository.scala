package unit.repositories

import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.api.mockito.PowerMockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.ExecutionContext
import testutil.TestImports  // Import the new helper

@PrepareForTest(Array(classOf[config.DatabaseConfig$]))
class TestableRepositorySpec extends AnyFlatSpec {
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  val mockConnection: Connection = PowerMockito.mock(classOf[Connection])
  
  // Use String class name instead of classOf
  PowerMockito.mockStatic(Class.forName(TestImports.DATABASE_CONFIG_CLASS))
  PowerMockito.when(config.DatabaseConfig.getConnection).thenReturn(mockConnection)
  
  // Your test code...
}

