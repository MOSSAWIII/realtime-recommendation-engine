package repositories

import models._
import scala.concurrent.{ExecutionContext, Future}

trait RepositoryInterface {
  def storeProcessedEvent(event: ProcessedEvent): Future[ProcessedEvent]
  def getProcessedEventsForUser(userId: Long, limit: Int = 100): Future[List[ProcessedEvent]]
  def storeRecommendation(recommendation: Recommendation): Future[Recommendation]
  def getRecommendationForUser(userId: Long): Future[Option[Recommendation]]
  def getRecentRecommendationForUser(userId: Long, timeWindow: Long): Future[Option[Recommendation]]
  def getRecentProcessedEvents(limit: Int): Future[Seq[ProcessedEvent]]
  def initTables()(implicit ec: ExecutionContext): Future[Unit]
}

