package common.mocks

import com.typesafe.config.{Config, ConfigFactory}
import infrastructure.config.{ConfigurationServiceInterface, ConfigSection}
import scala.concurrent.duration._

/**
 * Implémentation de ConfigurationServiceInterface pour les tests
 */
class TestConfigurationService(testConfig: Config = ConfigFactory.load("application-test.conf")) 
  extends ConfigurationServiceInterface {
  
  private var currentConfig = testConfig
  
  def get[T <: ConfigSection](implicit converter: Config => T): T = {
    converter(currentConfig)
  }
  
  def reload(): Boolean = true
  
  def getConfig: Config = currentConfig
  
  def updateConfigValue(path: String, value: String): Boolean = {
    // Implémentation simplifiée pour les tests
    true
  }
  
  def updateConfigValueGeneric(path: String, value: Any): Boolean = {
    // Implémentation simplifiée pour les tests
    true
  }
  
  def withUpdatedConfig(newConfig: Config): TestConfigurationService = {
    this.currentConfig = newConfig
    this
  }
  
  // Ajouter ces méthodes pour supporter les tests d'intégration
  def getDatabaseUrl(): String = "jdbc:postgresql://localhost:5433/test_recommendation_engine"
  def getDatabaseUser(): String = "postgres"
  def getDatabasePassword(): String = "test_password"
}