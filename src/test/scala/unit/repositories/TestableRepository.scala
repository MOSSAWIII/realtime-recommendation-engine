package unit.repositories

import org.powermock.core.classloader.annotations.PrepareForTest
import org.powermock.api.mockito.PowerMockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.ExecutionContext
import testutil.TestImports
import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner

@PrepareForTest(Array(classOf[config.DatabaseConfig.type]))
@RunWith(classOf[PowerMockRunner])
class TestableRepositorySpec extends AnyFlatSpec {
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  val mockConnection: Connection = PowerMockito.mock(classOf[Connection])
  
  // Use the correct Scala type reference for singleton objects
  PowerMockito.mockStatic(classOf[config.DatabaseConfig.type])
  PowerMockito.when(config.DatabaseConfig.getConnection).thenReturn(mockConnection)
  
  // Your test code...
}

