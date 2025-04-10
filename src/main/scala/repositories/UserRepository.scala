package repositories

import models._
import slick.jdbc.PostgresProfile.api._
import config.DatabaseConfig.db
import scala.concurrent.{ExecutionContext, Future}

class UserRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  def findAll(): Future[Seq[User]] = {
    executeWithErrorHandling(
      db.run(Tables.users.result),
      "Erreur lors de la récupération des utilisateurs"
    )
  }
  
  def findById(userId: Long): Future[Option[User]] = {
    executeWithErrorHandling(
      db.run(Tables.users.filter(_.userId === userId).result.headOption),
      s"Erreur lors de la récupération de l'utilisateur $userId"
    )
  }
  
  def findByIds(userIds: Seq[Long]): Future[Seq[User]] = {
    if (userIds.isEmpty) {
      Future.successful(Seq.empty)
    } else {
      executeWithErrorHandling(
        db.run(Tables.users.filter(_.userId inSet userIds).result),
        s"Erreur lors de la récupération des utilisateurs par IDs"
      )
    }
  }
  
  def upsert(user: User): Future[Int] = {
    executeWithErrorHandling(
      db.run(
        Tables.users.insertOrUpdate(user)
      ),
      s"Erreur lors de l'upsert de l'utilisateur ${user.userId}"
    )
  }
  
  def findByLocation(location: String): Future[Seq[User]] = {
    executeWithErrorHandling(
      db.run(Tables.users.filter(_.location === location).result),
      s"Erreur lors de la récupération des utilisateurs par localisation $location"
    )
  }
}