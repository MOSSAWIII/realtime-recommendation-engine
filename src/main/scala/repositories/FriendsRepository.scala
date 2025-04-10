package repositories

import models._
import slick.jdbc.PostgresProfile.api._
import config.DatabaseConfig.db
import scala.concurrent.{ExecutionContext, Future}

class FriendsRepository(implicit ec: ExecutionContext) extends BaseRepository {
  
  def findFriendsByUserId(userId: Long): Future[Seq[Long]] = {
    executeWithErrorHandling(
      db.run(Tables.userFriends.filter(_.userId === userId).map(_.friendId).result),
      s"Erreur lors de la récupération des amis pour l'utilisateur $userId"
    )
  }
  
  def areFriends(userId1: Long, userId2: Long): Future[Boolean] = {
    executeWithErrorHandling(
      db.run(
        Tables.userFriends
          .filter(f => f.userId === userId1 && f.friendId === userId2)
          .exists
          .result
      ),
      s"Erreur lors de la vérification de l'amitié entre $userId1 et $userId2"
    )
  }
  
  def upsert(friendship: UserFriends): Future[Int] = {
    executeWithErrorHandling(
      db.run(
        Tables.userFriends.insertOrUpdate(friendship)
      ),
      s"Erreur lors de l'upsert de l'amitié pour l'utilisateur ${friendship.userId}"
    )
  }
}

