package repositories

import models._
import scala.concurrent.{ExecutionContext, Future}

trait RepositoryInterface {
  implicit val ec: ExecutionContext
  
  // Event processing methods
  def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent]
  def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]]
  def getRecentProcessedEvents(limit: Int): Future[Seq[ProcessedEvent]]
  
  // Recommendation methods
  def storeRecommendation(recommendation: Recommendation): Future[Recommendation]
  def getRecommendationForUser(userId: Long): Future[Option[Recommendation]]
  def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]]
  
  // Database setup
  def initTables(): Future[Unit]
}

