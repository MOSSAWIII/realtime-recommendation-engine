package infrastructure.persistence

import scala.concurrent.{ExecutionContext, Future}
import java.sql.{Connection, PreparedStatement, ResultSet}

/**
 * Base trait for all repositories
 */
trait Repository {
  implicit val ec: ExecutionContext
  
  // Utility methods for SQL operations
  protected def withPreparedStatement[T](connection: Connection, sql: String)(f: PreparedStatement => T): T = {
    val stmt = connection.prepareStatement(sql)
    try {
      f(stmt)
    } finally {
      stmt.close()
    }
  }

  protected def withResultSet[T](stmt: PreparedStatement)(f: ResultSet => T): T = {
    val rs = stmt.executeQuery()
    try {
      f(rs)
    } finally {
      rs.close()
    }
  }
}