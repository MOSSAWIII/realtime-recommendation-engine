package utils.metrics

import scala.util.Try
import org.slf4j.LoggerFactory
import application.core.DataManager

/**
 * Classe dédiée aux health checks du DataManager
 */
object DataManagerHealthCheck {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Enregistre les health checks du DataManager dans le registry
   * @param dataManager instance du DataManager à vérifier
   */
  def registerHealthChecks(dataManager: DataManager): Unit = {
    // Health check pour la connectivité à la base de données
    HealthCheckRegistry.register(
      HealthCheckRegistry.createHealthCheck("database", "connection") {
        try {
          val isConnected = dataManager.isDatabaseAccessible()
          if (isConnected) {
            HealthCheckResult(
              component = "database",
              check = "connection",
              status = HealthStatus.Healthy,
              message = "Database connection is available"
            )
          } else {
            HealthCheckResult(
              component = "database",
              check = "connection",
              status = HealthStatus.Unhealthy,
              message = "Database connection is unavailable"
            )
          }
        } catch {
          case e: Exception =>
            HealthCheckResult(
              component = "database",
              check = "connection",
              status = HealthStatus.Unhealthy,
              message = s"Failed to check database connection: ${e.getMessage}"
            )
        }
      }
    )
    
    // Health check pour le cache
    HealthCheckRegistry.register(
      HealthCheckRegistry.createHealthCheck("cache", "availability") {
        try {
          val isCacheAvailable = dataManager.isCacheAccessible()
          if (isCacheAvailable) {
            HealthCheckResult(
              component = "cache",
              check = "availability",
              status = HealthStatus.Healthy,
              message = "Cache is available"
            )
          } else {
            HealthCheckResult(
              component = "cache",
              check = "availability",
              status = HealthStatus.Degraded,
              message = "Cache is unavailable, using fallback"
            )
          }
        } catch {
          case e: Exception =>
            HealthCheckResult(
              component = "cache",
              check = "availability",
              status = HealthStatus.Unhealthy,
              message = s"Failed to check cache availability: ${e.getMessage}"
            )
        }
      }
    )
  }
}

