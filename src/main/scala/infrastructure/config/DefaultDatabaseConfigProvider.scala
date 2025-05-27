package infrastructure.config

import java.sql.Connection
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

trait DatabaseConfigProvider {
  def getConnection(): Connection
  def close(): Unit
}

class DefaultDatabaseConfigProvider(configService: ConfigurationServiceInterface) extends DatabaseConfigProvider {
  private val dbConfig = new DatabaseConfigSection(
    configService.getConfig.getConfig("database"))
  private lazy val dataSource: HikariDataSource = createDataSource()
  
  private def createDataSource(): HikariDataSource = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setJdbcUrl(dbConfig.url)
    // ... rest of configuration
    new HikariDataSource(hikariConfig)
  }
  
  override def getConnection(): Connection = {
    dataSource.getConnection()
  }
  
  override def close(): Unit = {
    if (dataSource != null && !dataSource.isClosed) {
      dataSource.close()
    }
  }
}

