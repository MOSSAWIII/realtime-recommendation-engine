package infrastructure.config

import com.typesafe.config.Config

trait ConfigurationServiceInterface {
  def get[T <: ConfigSection](implicit converter: Config => T): T
  def reload(): Boolean
  def getConfig: Config
  def updateConfigValue(path: String, value: String): Boolean
  def updateConfigValueGeneric(path: String, value: Any): Boolean
  // Add other methods as needed
}

