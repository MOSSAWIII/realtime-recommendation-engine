package unit.repositories

import java.sql.{Connection, PreparedStatement, ResultSet}
import models.Event
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import repositories.EventRepository
import scala.concurrent.ExecutionContext.Implicits.global
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{verify, when}
import org.powermock.modules.junit4.PowerMockRunner
import org.junit.runner.RunWith

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[config.DatabaseConfig.type]))
class RepositorySpec extends AnyFunSuite with BeforeAndAfterEach {

  var mockConnection: Connection = _
  var mockPreparedStatement: PreparedStatement = _
  var mockResultSet: ResultSet = _

  override def beforeEach(): Unit = {
    mockConnection = PowerMockito.mock(classOf[Connection])
    mockPreparedStatement = PowerMockito.mock(classOf[PreparedStatement])
    mockResultSet = PowerMockito.mock(classOf[ResultSet])
    
    PowerMockito.mockStatic(classOf[config.DatabaseConfig.type])
    when(config.DatabaseConfig.getConnection).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(mockPreparedStatement)
  }
}

class EventRepositorySpec extends RepositorySpec {

  test("upsert should insert new event when no ID is provided") {
    val timestamp = System.currentTimeMillis()
    val event = Event(
      id = None,
      name = "Test Event",
      description = Some("Description"),
      startTime = timestamp,
      endTime = Some(timestamp + 3600000),
      location = Some("Location"),
      createdBy = 1L,
      status = "active"
    )
    
    when(mockPreparedStatement.executeUpdate()).thenReturn(1)
    
    val repo = new EventRepository()
    val result = repo.upsert(event)
    
    verify(mockConnection).prepareStatement(ArgumentMatchers.contains("INSERT INTO events"))
  }
  
  test("upsert should update existing event when ID is provided") {
    val timestamp = System.currentTimeMillis()
    val event = Event(
      id = Some(1L),
      name = "Test Event",
      description = Some("Description"),
      startTime = timestamp,
      endTime = Some(timestamp + 3600000),
      location = Some("Location"),
      createdBy = 1L,
      status = "active"
    )
    
    when(mockPreparedStatement.executeUpdate()).thenReturn(1)
    
    val repo = new EventRepository()
    val result = repo.upsert(event)
    
    verify(mockConnection).prepareStatement(ArgumentMatchers.contains("UPDATE events"))
  }
}