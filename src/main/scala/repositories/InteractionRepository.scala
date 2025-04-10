package repositories

import models._
import slick.jdbc.PostgresProfile.api._
import config.DatabaseConfig.db
import scala.concurrent.{ExecutionContext, Future}
import java.time.LocalDateTime

class InteractionRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  def save(interaction: UserInteraction): Future[Int] = {
    executeWithErrorHandling(
      db.run(Tables.userInteractions += interaction),
      s"Erreur lors de l'enregistrement de l'interaction"
    )
  }
  
  def saveBatch(interactions: Seq[UserInteraction]): Future[Option[Int]] = {
    if (interactions.isEmpty) {
      Future.successful(Some(0))
    } else {
      executeWithErrorHandling(
        db.run(Tables.userInteractions ++= interactions),
        s"Erreur lors de l'enregistrement des interactions en lot"
      )
    }
  }
  
  def findByUserId(userId: Long): Future[Seq[UserInteraction]] = {
    executeWithErrorHandling(
      db.run(Tables.userInteractions.filter(_.userId === userId).result),
      s"Erreur lors de la récupération des interactions pour l'utilisateur $userId"
    )
  }
  
  def findByEventId(eventId: Long): Future[Seq[UserInteraction]] = {
    executeWithErrorHandling(
      db.run(Tables.userInteractions.filter(_.eventId === eventId).result),
      s"Erreur lors de la récupération des interactions pour l'événement $eventId"
    )
  }
}