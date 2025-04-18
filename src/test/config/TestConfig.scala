package config

import com.typesafe.config.{Config, ConfigFactory}

// Configuration pour les tests qui peut être utilisée au lieu de Config réel
object TestConfig {
  // Force loading of test configuration
  lazy val config: Config = ConfigFactory.load("application-test.conf")
  
  // Access test-specific settings
  val enableMocking: Boolean = config.getBoolean("test.enableMocking")
}

