package repositories

import models._
import slick.jdbc.PostgresProfile.api._
import config.DatabaseConfig.db
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}

class ParticipationRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  // Helper function to parse user IDs stored as text
  private def parseUserIds(listText: Option[String]): Seq[Long] = {
    listText match {
      case Some(text) if text.nonEmpty => 
        text.split(",").flatMap(id => Try(id.trim.toLong).toOption)
      case _ => Seq.empty
    }
  }
  
  def findByEventId(eventId: Long): Future[EventParticipation] = {
    executeWithErrorHandling(
      db.run(Tables.eventParticipations.filter(_.eventId === eventId).result.headOption).map {
        case Some(participation) => participation
        case None => EventParticipation(eventId)
      },
      s"Error retrieving participation for event $eventId"
    )
  }
  
  def findEventsByUserId(userId: Long): Future[Seq[Long]] = {
    val userIdStr = s"$userId"
    
    // This query is complex as we need to check all four list columns
    executeWithErrorHandling(
      db.run(
        sql"""
          SELECT event_id FROM participation_evenement 
          WHERE yes_list LIKE '%'||$userIdStr||'%' 
             OR maybe_list LIKE '%'||$userIdStr||'%'
             OR invited_list LIKE '%'||$userIdStr||'%'
             OR no_list LIKE '%'||$userIdStr||'%'
        """.as[Long]
      ),
      s"Error retrieving events for user $userId"
    )
  }
  
  def countParticipatingByEventId(eventId: Long): Future[Int] = {
    executeWithErrorHandling(
      findByEventId(eventId).map(participation => 
        parseUserIds(participation.yesList).size + 
        parseUserIds(participation.maybeList).size
      ),
      s"Error counting participants for event $eventId"
    )
  }
  
  // Other repository methods...
}

