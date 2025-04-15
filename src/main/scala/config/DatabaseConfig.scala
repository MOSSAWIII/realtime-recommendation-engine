package config

import com.typesafe.config.ConfigFactory
import java.sql.Connection
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.slf4j.LoggerFactory

object DatabaseConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  private val config = ConfigFactory.load()
  
  private val dbUrl = config.getString("database.url")
  private val dbUser = config.getString("database.user")
  private val dbPassword = config.getString("database.password")
  
  // Chargement du driver PostgreSQL
  Class.forName("org.postgresql.Driver")
  
  // Configuration du pool de connexions
  private val hikariConfig = new HikariConfig()
  hikariConfig.setJdbcUrl(dbUrl)
  hikariConfig.setUsername(dbUser)
  hikariConfig.setPassword(dbPassword)
  hikariConfig.setMaximumPoolSize(10)
  hikariConfig.setMinimumIdle(2)
  hikariConfig.setIdleTimeout(30000)
  hikariConfig.setPoolName("RecommendationEnginePool")
  
  private val dataSource = new HikariDataSource(hikariConfig)
  
  logger.info(s"Connection pool initialized with size: ${hikariConfig.getMaximumPoolSize}")
  
  def getConnection(): Connection = {
    dataSource.getConnection()
  }
  
  def closePool(): Unit = {
    if (dataSource != null && !dataSource.isClosed) {
      dataSource.close()
      logger.info("Connection pool closed")
    }
  }
}