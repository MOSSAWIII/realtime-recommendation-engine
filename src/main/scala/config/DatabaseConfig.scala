package config

import slick.jdbc.PostgresProfile.api._
import com.typesafe.config.ConfigFactory

object DatabaseConfig {
  private val config = ConfigFactory.load()
  
  val db = Database.forConfig("postgres")
  
  // Pour fermer proprement les connexions à l'arrêt de l'application
  sys.addShutdownHook {
    db.close()
  }
}