package repositories

import models._
import slick.jdbc.PostgresProfile.api._
import config.DatabaseConfig.db
import scala.concurrent.{ExecutionContext, Future}
import java.time.LocalDateTime

class EventRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  def findFutureEvents(): Future[Seq[Event]] = {
    val now = LocalDateTime.now()
    
    executeWithErrorHandling(
      db.run(Tables.events.filter(_.startTime > now).result),
      "Error retrieving future events"
    )
  }
  
  def findById(eventId: Long): Future[Option[Event]] = {
    executeWithErrorHandling(
      db.run(Tables.events.filter(_.eventId === eventId).result.headOption),
      s"Error retrieving event $eventId"
    )
  }
  
  def findByIds(eventIds: Seq[Long]): Future[Seq[Event]] = {
    if (eventIds.isEmpty) {
      Future.successful(Seq.empty)
    } else {
      executeWithErrorHandling(
        db.run(Tables.events.filter(_.eventId inSet eventIds).result),
        s"Error retrieving events by IDs"
      )
    }
  }
  
  def findByKeywords(keywordIds: Seq[Int]): Future[Seq[Event]] = {
    val now = LocalDateTime.now()
    
    // Filter in memory, as filtering on the keyword map is complex in SQL
    executeWithErrorHandling(
      db.run(Tables.events.filter(_.startTime > now).result)
        .map(events => 
          events.filter(event => 
            keywordIds.exists(id => event.keywordFrequencies.getOrElse(id, 0) > 0)
          )
        ),
      "Error retrieving events by keywords"
    )
  }
  
  def findByLocation(location: String): Future[Seq[Event]] = {
    val now = LocalDateTime.now()
    
    executeWithErrorHandling(
      db.run(
        Tables.events
          .filter(_.startTime > now)
          .filter(e => 
            e.city === location || 
            e.state === location || 
            e.country === location
          )
          .result
      ),
      s"Error retrieving events by location $location"
    )
  }
  
  def upsert(event: Event): Future[Int] = {
    executeWithErrorHandling(
      db.run(
        Tables.events.insertOrUpdate(event)
      ),
      s"Error upserting event ${event.eventId}"
    )
  }
}