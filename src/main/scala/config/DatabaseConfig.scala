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
  hikariConfig.setUsername(config.getString("username"))
  hikariConfig.setPassword(config.getString("password"))
  hikariConfig.setMaximumPoolSize(config.getInt("poolSize"))
  hikariConfig.setConnectionTimeout(config.getLong("connectionTimeout"))
  
  private val dataSource = new HikariDataSource(hikariConfig)
  
  def getConnection(): Connection = {
    dataSource.getConnection()
  }
  
  def closePool(): Unit = {
    if (dataSource != null && !dataSource.isClosed) {
      dataSource.close()
    }
  }
}