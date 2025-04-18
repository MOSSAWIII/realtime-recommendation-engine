package unit.repositories

import repositories.Repository
import java.sql.Connection
import scala.concurrent.ExecutionContext

class TestableRepository(mockConnection: Connection)(override implicit val ec: ExecutionContext) extends Repository()(ec) {
  // Ne pas utiliser 'override' si la méthode n'existe pas dans la classe parente
  protected def withConnection[T](operation: Connection => T): T = {
    operation(mockConnection)
  }
}

