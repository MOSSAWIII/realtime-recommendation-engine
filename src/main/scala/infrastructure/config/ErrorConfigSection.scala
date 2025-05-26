package infrastructure.config

import com.typesafe.config.Config

/**
 * Configuration section for error handling
 */
class ErrorConfigSection(config: Config) extends ConfigSection {
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
  
  override def validate(): Map[String, String] = {
    val errors = scala.collection.mutable.Map[String, String]()
    
    if (maxRecentErrors <= 0) {
      errors += ("error.maxRecentErrors" -> s"Max recent errors must be positive, got $maxRecentErrors")
    }
    
    if (defaultMaxRetries < 0) {
      errors += ("error.retry.defaultMaxRetries" -> s"Default max retries must be non-negative, got $defaultMaxRetries")
    }
    
    errors.toMap
  }
}