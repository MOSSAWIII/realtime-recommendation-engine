package config

import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import java.sql.{Connection, PreparedStatement, ResultSet}

object TestDatabaseConfig {
  // For unit tests, provide a mock connection
  def getMockConnection(): Connection = {
    val mockConnection = mock(classOf[Connection])
    val mockStatement = mock(classOf[PreparedStatement])
    val mockResultSet = mock(classOf[ResultSet])
    
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockStatement)
    when(mockStatement.executeQuery()).thenReturn(mockResultSet)
    
    mockConnection
  }
}

