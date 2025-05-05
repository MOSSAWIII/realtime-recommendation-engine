package repositories

import models._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Default implementation of Repository interface for testing or development purposes.
 * Provides no-op implementations of all methods.
 */
class DefaultRepository(implicit override val ec: ExecutionContext) extends RepositoryInterface {
  
  // Event processing methods
  override def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent] = 
    Future.successful(event.copy(id = Some(1L)))
    
  override def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]] = 
    Future.successful(List.empty)
    
  override def getRecentProcessedEvents(limit: Int): Future[Seq[ProcessedEvent]] = 
    Future.successful(Seq.empty)
    
  // Recommendation methods
  override def storeRecommendation(recommendation: Recommendation): Future[Recommendation] =
    Future.successful(recommendation.copy(id = Some(1L)))
    
  override def getRecommendationForUser(userId: Long): Future[Option[Recommendation]] =
    Future.successful(None)
    
  override def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]] =
    Future.successful(None)
    
  // Database setup
  override def initTables(): Future[Unit] = Future.successful(())
}

