package infrastructure.config

import com.typesafe.config.Config
import scala.concurrent.duration._

/**
 * Configuration for metrics and health checks
 */
class MetricsConfigSection(config: Config) {
  private val metricsConfig = config.getConfig("metrics")
  
  // Reporting configuration
  val enabled: Boolean = metricsConfig.getBoolean("enabled")
  val reportingIntervalSeconds: Int = metricsConfig.getInt("reportingIntervalSeconds")
  val logDetailedMetrics: Boolean = metricsConfig.getBoolean("logDetailedMetrics")
  
  // External reporting
  val jmxEnabled: Boolean = metricsConfig.getBoolean("jmx.enabled")
  val prometheusEnabled: Boolean = metricsConfig.getBoolean("prometheus.enabled")
  val prometheusPort: Int = if (prometheusEnabled) metricsConfig.getInt("prometheus.port") else 0
  
  // Health check configuration
  val healthCheckEnabled: Boolean = metricsConfig.getBoolean("healthCheck.enabled")
  val healthCheckIntervalSeconds: Int = metricsConfig.getInt("healthCheck.intervalSeconds")
  val healthCheckTimeoutMs: Int = metricsConfig.getInt("healthCheck.timeoutMs")
}