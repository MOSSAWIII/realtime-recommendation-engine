package infrastructure.config

import com.typesafe.config.Config

/**
 * Configuration section for error handling
 */
class ErrorConfigSection(config: Config) {
  private val errorConfig = config.getConfig("error")
  
  val enableEnhancedLogging: Boolean = errorConfig.getBoolean("enableEnhancedLogging")
  val reportingEnabled: Boolean = errorConfig.getBoolean("reportingEnabled")
  val stackTraceEnabled: Boolean = errorConfig.getBoolean("stackTraceEnabled")
  val maxRecentErrors: Int = errorConfig.getInt("maxRecentErrors")
  
  // Retry configuration
  val defaultMaxRetries: Int = errorConfig.getInt("retry.defaultMaxRetries")
  val defaultInitialBackoffMs: Long = errorConfig.getLong("retry.defaultInitialBackoffMs")
  val defaultMaxBackoffMs: Long = errorConfig.getLong("retry.defaultMaxBackoffMs")
  val defaultBackoffFactor: Double = errorConfig.getDouble("retry.defaultBackoffFactor")
  
  // Circuit breaker configuration
  val defaultFailureThreshold: Int = errorConfig.getInt("circuitBreaker.defaultFailureThreshold")
  val defaultResetTimeoutMs: Long = errorConfig.getLong("circuitBreaker.defaultResetTimeoutMs")
}