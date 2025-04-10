package config

import slick.jdbc.PostgresProfile.api._
import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object DatabaseConfig {
  private val config = ConfigFactory.load()
  
  private val dbConfig = new HikariConfig()
  dbConfig.setJdbcUrl(config.getString("database.url"))
  dbConfig.setUsername(config.getString("database.user"))
  dbConfig.setPassword(config.getString("database.password"))
  dbConfig.setMaximumPoolSize(config.getInt("database.poolSize"))
  dbConfig.addDataSourceProperty("cachePrepStmts", "true")
  dbConfig.addDataSourceProperty("prepStmtCacheSize", "250")
  dbConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  
  val db = Database.forDataSource(new HikariDataSource(dbConfig), None)
  
  def close(): Unit = {
    db.close()
  }

  // Ajouter ces méthodes pour améliorer la gestion des transactions
  def withTransaction[T](operations: DBIO[T]): Future[T] = {
    db.run(operations.transactionally)
  }

  // Ajouter une méthode pour les opérations en lecture seule
  def withReadOnly[T](operations: DBIO[T]): Future[T] = {
    db.run(operations)
  }

  // Ajouter cette méthode pour vérifier l'état de la connexion
  def healthCheck(): Future[Boolean] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    db.run(sql"SELECT 1".as[Int]).map(_ => true).recover { case _ => false }
  }
}