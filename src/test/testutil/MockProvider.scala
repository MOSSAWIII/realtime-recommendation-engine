package testutil

import java.sql.{Connection, PreparedStatement, ResultSet}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._

object MockProvider {
  def createMockDbConnection(): Connection = {
    val conn = mock(classOf[Connection])
    val stmt = mock(classOf[PreparedStatement])
    val rs = mock(classOf[ResultSet])
    
    when(conn.prepareStatement(any())).thenReturn(stmt)
    when(stmt.executeQuery()).thenReturn(rs)
    // Add more mock behaviors as needed
    
    conn
  }
}

