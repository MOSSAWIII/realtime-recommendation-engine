package unit.infrastructure.persistence

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

class Repository(connection: => Connection)(implicit val ec: ExecutionContext) {
  def executeQuery(query: String): Future[Boolean] = {
    Future {
      val conn = connection
      val stmt = conn.prepareStatement(query)
      val rs = stmt.executeQuery()
      rs.next()
    }
  }
}

