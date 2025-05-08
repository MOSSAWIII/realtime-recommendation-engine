package config

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.Connection
import com.typesafe.config.ConfigFactory

object DatabaseConfig {
  private val config = ConfigFactory.load()
  
  // Check if we're in test mode
  private val isTestMode = try {
    if (System.getProperty("test") != null) 
      System.getProperty("test").toBoolean
    else 
      config.hasPath("test.mode") && config.getBoolean("test.mode")
  } catch {
    case _: Exception => false
  }
  
  private val dbUrl = config.getString("database.url")
  private val dbUser = config.getString("database.user")
  private val dbPassword = config.getString("database.password")
  
  private val hikariConfig = new HikariConfig()
  hikariConfig.setJdbcUrl(dbUrl)
  hikariConfig.setUsername(dbUser)
  hikariConfig.setPassword(dbPassword)
  hikariConfig.setMaximumPoolSize(10)
  hikariConfig.setMinimumIdle(2)
  
  private val dataSource = new HikariDataSource(hikariConfig)
  
  def getConnection(): Connection = {
    dataSource.getConnection()
  }
  
  def close(): Unit = {
    dataSource.close()
  }
}