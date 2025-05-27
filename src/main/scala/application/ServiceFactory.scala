package application

import application.core.{DataManager, Recommender, Transformer}
import infrastructure.config.{ConfigurationService, ConfigurationServiceInterface, DefaultDatabaseConfigProvider, DatabaseConfigProvider}
import scala.concurrent.ExecutionContext

class ServiceFactory(configService: ConfigurationServiceInterface)(implicit ec: ExecutionContext) {
  
  lazy val databaseProvider: DatabaseConfigProvider = new DefaultDatabaseConfigProvider(configService)
  
  lazy val dataManager: DataManager = new DataManager(configService, databaseProvider)
  
  lazy val transformer: Transformer = new Transformer(dataManager)
  
  lazy val recommender: Recommender = new Recommender(dataManager)
}

object ServiceFactory {
  def createDefault(implicit ec: ExecutionContext): ServiceFactory = {
    // For backward compatibility
    new ServiceFactory(ConfigurationService.getInstance)
  }
}

