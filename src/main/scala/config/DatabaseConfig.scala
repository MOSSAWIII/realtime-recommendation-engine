package config

import com.typesafe.config.ConfigFactory
import java.sql.{Connection, DriverManager}

object DatabaseConfig {
  private val config = ConfigFactory.load()
  
  private val dbUrl = config.getString("database.url")
  private val dbUser = config.getString("database.user")
  private val dbPassword = config.getString("database.password")
  
  // Chargement du driver PostgreSQL
  Class.forName("org.postgresql.Driver")
  
  def getConnection(): Connection = {
    DriverManager.getConnection(dbUrl, dbUser, dbPassword)
  }
}