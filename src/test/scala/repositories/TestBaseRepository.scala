package repositories

import java.sql.Connection
import scala.concurrent.ExecutionContext
import config.TestDatabaseConfig

trait TestBaseRepository extends BaseRepository {
  // Override connection methods for testing
  protected def withConnection[T](operation: Connection => T): T = {
    val mockConnection = TestDatabaseConfig.getMockConnection()
    try {
      operation(mockConnection)
    } finally {
      // No need to close mock connection
    }
  }
}

