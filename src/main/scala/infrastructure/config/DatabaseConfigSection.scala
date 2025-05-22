package infrastructure.config

import com.typesafe.config.Config
import scala.util.Try
import java.sql.DriverManager
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Typed database configuration section
 */
class DatabaseConfigSection(config: Config) extends ConfigSection {
  private val dbConfig = config.getConfig("database")
  
  val url: String = dbConfig.getString("url")
  val user: String = dbConfig.getString("user")
  val password: String = dbConfig.getString("password")
  val poolSize: Int = if (dbConfig.hasPath("poolSize")) dbConfig.getInt("poolSize") else 10
  val connectionTimeout: FiniteDuration = 
    if (dbConfig.hasPath("connectionTimeout")) 
      Duration(dbConfig.getString("connectionTimeout")).asInstanceOf[FiniteDuration]
    else
      Duration("30s").asInstanceOf[FiniteDuration]
  val idleTimeout: FiniteDuration = 
    if (dbConfig.hasPath("idleTimeout")) 
      Duration(dbConfig.getString("idleTimeout")).asInstanceOf[FiniteDuration]
    else
      Duration("10m").asInstanceOf[FiniteDuration]
  val maxLifetime: FiniteDuration = 
    if (dbConfig.hasPath("maxLifetime")) 
      Duration(dbConfig.getString("maxLifetime")).asInstanceOf[FiniteDuration]
    else
      Duration("30m").asInstanceOf[FiniteDuration]
  
  override def validate(): Map[String, String] = {
    val errors = scala.collection.mutable.Map[String, String]()
    
    if (url.isEmpty) {
      errors += ("database.url" -> "Database URL cannot be empty")
    }
    
    if (user.isEmpty) {
      errors += ("database.user" -> "Database user cannot be empty")
    }
    
    if (poolSize < 1) {
      errors += ("database.poolSize" -> s"Pool size must be at least 1, got $poolSize")
    }
    
    // Optional: Validate database connection if needed
    if (errors.isEmpty && config.getBoolean("database.validateOnStartup")) {
      Try {
        val conn = DriverManager.getConnection(url, user, password)
        try {
          conn.isValid(5)
        } finally {
          conn.close()
        }
      }.failed.foreach { ex =>
        errors += ("database.connection" -> s"Failed to validate database connection: ${ex.getMessage}")
      }
    }
    
    errors.toMap
  }
}

