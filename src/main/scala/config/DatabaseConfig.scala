package config

import java.sql.Connection
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import com.typesafe.config.ConfigFactory

/**
 * Database connection configuration using HikariCP
 */
object DatabaseConfig {
  private val config = ConfigFactory.load().getConfig("database")
  
  private val hikariConfig = new HikariConfig()
  hikariConfig.setJdbcUrl(config.getString("url"))
  hikariConfig.setUsername(config.getString("user")) // Changed from "username" to match config
  hikariConfig.setPassword(config.getString("password"))
  hikariConfig.setMaximumPoolSize(config.getInt("poolSize"))
  // Only set connectionTimeout if it exists in config
  if (config.hasPath("connectionTimeout")) {
    hikariConfig.setConnectionTimeout(config.getLong("connectionTimeout"))
  }
  
  private val dataSource = new HikariDataSource(hikariConfig)
  
  // Remove the parentheses to make it consistent
  def getConnection: Connection = {
    dataSource.getConnection()
  }
  
  def closePool(): Unit = {
    if (dataSource != null && !dataSource.isClosed) {
      dataSource.close()
    }
  }
}