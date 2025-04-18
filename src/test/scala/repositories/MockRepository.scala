package repositories

import models._
import scala.concurrent.{ExecutionContext, Future}
import java.sql.Connection  // Ajout de l'import manquant

class MockRepository(implicit val ec: ExecutionContext) extends RepositoryInterface {
  // In-memory storage for unit tests
  private var events = List.empty[ProcessedEvent]
  private var recommendations = Map.empty[Long, Recommendation]

  def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = {
    Future.successful(event.copy(id = Some(1L)))
  }

  def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = {
    Future.successful(List.empty)
  }

  def storeRecommendation(recommendation: Recommendation): Future[Recommendation] = {
    Future.successful(recommendation.copy(id = Some(1L)))
  }

  def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] = {
    Future.successful(None)
  }

  def getRecentProcessedEvents(limit: Int): Future[Seq[ProcessedEvent]] = {
    Future.successful(Seq.empty)
  }

  def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] = {
    Future.successful(None)
  }

  def initTables()(implicit ec: ExecutionContext): Future[Unit] = {
    Future.successful(())
  }

  protected def withConnection[T](operation: Connection => T): T = {
    operation(null) // Ou utiliser Mockito pour créer un mock Connection
  }
}

