package infrastructure.config

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.sql.Connection
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

/**
 * Enhanced database configuration with ConfigurationService integration
 */
object DatabaseConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  private var dataSource: Option[HikariDataSource] = None
  
  // Initialize data source on first access
  private def getDataSource: HikariDataSource = synchronized {
    if (dataSource.isEmpty) {
      logger.info("Initializing database connection pool")
      val dbConfig = ConfigurationService.getInstance.getConfigSection(new DatabaseConfigSection(_))
      
      val hikariConfig = new HikariConfig()
      hikariConfig.setJdbcUrl(dbConfig.url)
      hikariConfig.setUsername(dbConfig.user)
      hikariConfig.setPassword(dbConfig.password)
      hikariConfig.setMaximumPoolSize(dbConfig.poolSize)
      hikariConfig.setConnectionTimeout(dbConfig.connectionTimeout.toMillis)
      hikariConfig.setIdleTimeout(dbConfig.idleTimeout.toMillis)
      hikariConfig.setMaxLifetime(dbConfig.maxLifetime.toMillis)
      
      Try {
        val ds = new HikariDataSource(hikariConfig)
        dataSource = Some(ds)
        ds
      } match {
        case Success(ds) => 
          logger.info(s"Database connection pool initialized with size ${dbConfig.poolSize}")
          ds
        case Failure(ex) =>
          logger.error("Failed to initialize database connection pool", ex)
          throw ex
      }
    } else {
      dataSource.get
    }
  }
  
  /**
   * Get a database connection from the pool
   *
   * @return A database connection
   */
  def getConnection: Connection = {
    getDataSource.getConnection()
  }
  
  /**
   * Close the database connection pool
   */
  def close(): Unit = synchronized {
    dataSource.foreach { ds =>
      logger.info("Closing database connection pool")
      ds.close()
      dataSource = None
    }
  }
  
  /**
   * Create a listener to react to database configuration changes
   */
  def listenForConfigChanges(): Unit = {
    val configService = ConfigurationService.getInstance
    
    configService.addChangeListener("database", new ConfigChangeListener {
      override def onConfigChanged(newConfig: com.typesafe.config.Config): Unit = {
        logger.info("Database configuration changed, reinitializing connection pool")
        close()
      }
    })
  }
}