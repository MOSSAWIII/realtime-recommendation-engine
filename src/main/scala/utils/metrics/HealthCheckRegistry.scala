package utils.metrics

import java.util.concurrent.{ConcurrentHashMap, TimeUnit, Executors, ScheduledExecutorService}
import java.util.concurrent.atomic.{AtomicReference, AtomicInteger}
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.JavaConverters._
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import infrastructure.config.ConfigurationService
import infrastructure.config.MetricsConfigSection
import java.time.Instant

/**
 * Health check status
 */
object HealthStatus extends Enumeration {
  type HealthStatus = Value
  val Healthy, Degraded, Unhealthy, Unknown = Value
}

/**
 * Health check result
 */
case class HealthCheckResult(
  component: String,
  check: String,
  status: HealthStatus.HealthStatus,
  message: String,
  details: Map[String, String] = Map.empty,
  timestamp: Long = System.currentTimeMillis()
) {
  def formattedTimestamp: String = Instant.ofEpochMilli(timestamp).toString
  
  def toMap: Map[String, String] = {
    Map(
      "component" -> component,
      "check" -> check,
      "status" -> status.toString,
      "message" -> message,
      "timestamp" -> formattedTimestamp
    ) ++ details
  }
}

/**
 * Health check interface
 */
trait HealthCheck {
  def name: String
  def component: String
  def execute(): HealthCheckResult
}

/**
 * Registry for managing and running health checks
 */
object HealthCheckRegistry {
  private val logger = LoggerFactory.getLogger(getClass)
  private val healthChecks = new ConcurrentHashMap[String, HealthCheck]()
  private val results = new ConcurrentHashMap[String, HealthCheckResult]()
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private var initialized = false
  
  // Execution context for health checks
  private implicit val executionContext: ExecutionContext = 
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  
  /**
   * Initialize health check registry
   */
  def init(): Unit = synchronized {
    if (initialized) return
    
    logger.info("Initializing health check registry")
    
    // Start periodic health check execution
    try {
      val config = ConfigurationService.getInstance.getConfigSection(new MetricsConfigSection(_))
      val checkIntervalSeconds = config.healthCheckIntervalSeconds
      
      if (checkIntervalSeconds > 0) {
        scheduler.scheduleAtFixedRate(
          () => runAllHealthChecks(),
          checkIntervalSeconds,
          checkIntervalSeconds,
          TimeUnit.SECONDS
        )
        logger.info(s"Health checks scheduled every $checkIntervalSeconds seconds")
      }
    } catch {
      case e: Exception =>
        logger.warn("Failed to read health check configuration, using defaults", e)
        // Default check interval
        scheduler.scheduleAtFixedRate(
          () => runAllHealthChecks(),
          60,
          60,
          TimeUnit.SECONDS
        )
    }
    
    initialized = true
  }
  
  /**
   * Register a health check
   */
  def register(healthCheck: HealthCheck): Unit = {
    val key = formatKey(healthCheck.component, healthCheck.name)
    healthChecks.put(key, healthCheck)
    logger.debug(s"Registered health check: $key")
  }
  
  /**
   * Unregister a health check
   */
  def unregister(component: String, name: String): Unit = {
    val key = formatKey(component, name)
    healthChecks.remove(key)
    results.remove(key)
    logger.debug(s"Unregistered health check: $key")
  }
  
  /**
   * Format key for health check lookup
   */
  private def formatKey(component: String, name: String): String = {
    s"$component:$name"
  }
  
  /**
   * Parse key into component and name
   */
  private def parseKey(key: String): (String, String) = {
    val parts = key.split(":")
    if (parts.length >= 2) {
      (parts(0), parts(1))
    } else {
      (key, "unknown")
    }
  }
  
  /**
   * Run all registered health checks
   */
  def runAllHealthChecks(): Unit = {
    logger.debug("Running all health checks")
    
    val futures = healthChecks.asScala.map { case (key, healthCheck) =>
      Future {
        try {
          val result = healthCheck.execute()
          results.put(key, result)
          
          // Log based on status
          result.status match {
            case HealthStatus.Healthy => 
              logger.debug(s"Health check passed: ${healthCheck.component}/${healthCheck.name}")
            case HealthStatus.Degraded => 
              logger.warn(s"Health check degraded: ${healthCheck.component}/${healthCheck.name} - ${result.message}")
            case HealthStatus.Unhealthy => 
              logger.error(s"Health check failed: ${healthCheck.component}/${healthCheck.name} - ${result.message}")
            case HealthStatus.Unknown => 
              logger.warn(s"Health check unknown: ${healthCheck.component}/${healthCheck.name} - ${result.message}")
          }
          
          result
        } catch {
          case e: Exception =>
            val errorResult = HealthCheckResult(
              component = healthCheck.component,
              check = healthCheck.name,
              status = HealthStatus.Unhealthy,
              message = s"Exception during health check: ${e.getMessage}",
              details = Map("exception" -> e.getClass.getName)
            )
            results.put(key, errorResult)
            logger.error(s"Exception in health check ${healthCheck.component}/${healthCheck.name}", e)
            errorResult
        }
      }
    }
    
    // Wait for all checks to complete (with timeout)
    try {
      import scala.concurrent.Future
      val future = Future.sequence(futures.toSeq)
      Await.result(future, 10.seconds)
    } catch {
      case e: Exception =>
        logger.error("Error waiting for health check completion", e)
    }
  }
  
  /**
   * Run a specific health check
   */
  def runHealthCheck(component: String, name: String): Option[HealthCheckResult] = {
    val key = formatKey(component, name)
    Option(healthChecks.get(key)).map { healthCheck =>
      try {
        val result = healthCheck.execute()
        results.put(key, result)
        result
      } catch {
        case e: Exception =>
          val errorResult = HealthCheckResult(
            component = healthCheck.component,
            check = healthCheck.name,
            status = HealthStatus.Unhealthy,
            message = s"Exception during health check: ${e.getMessage}",
            details = Map("exception" -> e.getClass.getName)
          )
          results.put(key, errorResult)
          errorResult
      }
    }
  }
  
  /**
   * Get the most recent result for a health check
   */
  def getHealthCheckResult(component: String, name: String): Option[HealthCheckResult] = {
    val key = formatKey(component, name)
    Option(results.get(key))
  }
  
  /**
   * Get all health check results
   */
  def getAllHealthCheckResults(): Map[String, HealthCheckResult] = {
    results.asScala.toMap
  }
  
  /**
   * Get all health check results for a component
   */
  def getComponentHealthCheckResults(component: String): Map[String, HealthCheckResult] = {
    results.asScala.filter { case (key, _) =>
      parseKey(key)._1 == component
    }.toMap
  }
  
  /**
   * Get overall system health status
   */
  def getSystemHealth(): HealthStatus.HealthStatus = {
    if (results.isEmpty) {
      return HealthStatus.Unknown
    }
    
    var hasUnhealthy = false
    var hasDegraded = false
    var hasHealthy = false
    
    results.values().asScala.foreach { result =>
      result.status match {
        case HealthStatus.Unhealthy => hasUnhealthy = true
        case HealthStatus.Degraded => hasDegraded = true
        case HealthStatus.Healthy => hasHealthy = true
        case _ => // Ignore Unknown status
      }
    }
    
    if (hasUnhealthy) {
      HealthStatus.Unhealthy
    } else if (hasDegraded) {
      HealthStatus.Degraded
    } else if (hasHealthy) {
      HealthStatus.Healthy
    } else {
      HealthStatus.Unknown
    }
  }
  
  /**
   * Get component health status
   */
  def getComponentHealth(component: String): HealthStatus.HealthStatus = {
    val componentResults = getComponentHealthCheckResults(component)
    
    if (componentResults.isEmpty) {
      return HealthStatus.Unknown
    }
    
    var hasUnhealthy = false
    var hasDegraded = false
    var hasHealthy = false
    
    componentResults.values.foreach { result =>
      result.status match {
        case HealthStatus.Unhealthy => hasUnhealthy = true
        case HealthStatus.Degraded => hasDegraded = true
        case HealthStatus.Healthy => hasHealthy = true
        case _ => // Ignore Unknown status
      }
    }
    
    if (hasUnhealthy) {
      HealthStatus.Unhealthy
    } else if (hasDegraded) {
      HealthStatus.Degraded
    } else if (hasHealthy) {
      HealthStatus.Healthy
    } else {
      HealthStatus.Unknown
    }
  }
  
  /**
   * Get health check summary
   */
  def getHealthSummary(): Map[String, Any] = {
    val componentMap = MutableMap[String, Any]()
    val allChecks = getAllHealthCheckResults()
    
    // Group by component
    allChecks.values.groupBy(_.component).foreach { case (component, checks) =>
      val checksMap = checks.map { check =>
        check.check -> Map(
          "status" -> check.status.toString,
          "message" -> check.message,
          "timestamp" -> check.formattedTimestamp,
          "details" -> check.details
        )
      }.toMap
      
      componentMap(component) = checksMap
    }
    
    val overallStatus = getSystemHealth()
    
    Map(
      "status" -> overallStatus.toString,
      "timestamp" -> Instant.now().toString,
      "components" -> componentMap.toMap
    )
  }
  
  /**
   * Create a formatted health report string
   */
  def getHealthReport(): String = {
    val sb = new StringBuilder("System Health Report:\n")
    sb.append(s"Overall Status: ${getSystemHealth()}\n")
    sb.append(s"Report Time: ${Instant.now()}\n\n")
    
    // Group by component
    getAllHealthCheckResults().values.groupBy(_.component).foreach { case (component, checks) =>
      sb.append(s"Component: $component (${getComponentHealth(component)})\n")
      
      checks.toSeq.sortBy(_.check).foreach { check =>
        sb.append(f"  [${check.status}%-9s] ${check.check}: ${check.message}\n")
        
        if (check.details.nonEmpty) {
          check.details.foreach { case (k, v) =>
            sb.append(f"    - $k: $v\n")
          }
        }
      }
      
      sb.append("\n")
    }
    
    sb.toString()
  }
  
  /**
   * Create a standard health check that executes the given function
   */
  def createHealthCheck(component: String, name: String)
                      (checkFn: => HealthCheckResult): HealthCheck = {
    new HealthCheck {
      override def component: String = component
      override def name: String = name
      override def execute(): HealthCheckResult = checkFn
    }
  }
  
  /**
   * Reset health check results
   */
  def resetResults(): Unit = {
    results.clear()
  }
  
  /**
   * Shutdown health check registry
   */
  def shutdown(): Unit = {
    logger.info("Shutting down health check registry")
    scheduler.shutdown()
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow()
      }
    } catch {
      case _: InterruptedException => 
        scheduler.shutdownNow()
    }
  }
}