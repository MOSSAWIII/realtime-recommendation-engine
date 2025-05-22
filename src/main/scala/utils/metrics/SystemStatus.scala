package utils.metrics

import infrastructure.messaging.rabbitmq.RabbitMQManager
import infrastructure.config.ConfigurationService
import com.rabbitmq.client.Channel
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import org.slf4j.LoggerFactory
import scala.collection.mutable.{Map => MutableMap}

/**
 * Utility class to monitor system status and message flow
 * Enhanced to use the MetricsRegistry for consistent metrics collection
 */
object SystemStatus {
  private val logger = LoggerFactory.getLogger(getClass)
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  
  // Legacy queue stats tracking
  private val queueStats = MutableMap[String, QueueStats]()
  
  // Metrics registry metadata
  private val COMPONENT = "system"
  private val QUEUE_MESSAGE_COUNT = "queue.message.count"
  private val QUEUE_CONSUMER_COUNT = "queue.consumer.count"
  private val QUEUE_PROCESSING_TIME = "queue.processing.time"
  
  private val MONITORED_QUEUES = Seq(
    RabbitMQManager.Queues.PROCESSED_EVENTS,
    RabbitMQManager.Queues.RECOMMENDATIONS,
    RabbitMQManager.Queues.HISTORICAL_DATA
  )
  
  case class QueueStats(
    name: String,
    messageCount: Long = 0,
    consumerCount: Int = 0,
    avgProcessingTime: Long = 0,
    lastUpdated: Long = System.currentTimeMillis()
  )
  
  /**
   * Start monitoring system status
   */
  def startMonitoring(): Unit = {
    logger.info("Starting system status monitoring")
    
    // Initialize metrics registry if not already done
    if (!MetricsRegistry.isInitialized) {
      MetricsRegistry.init()
    }
    
    // Register health checks
    registerHealthChecks()
    
    // Schedule periodic queue stats collection
    scheduler.scheduleAtFixedRate(
      () => collectQueueStats(),
      10, // Initial delay
      30, // Period
      TimeUnit.SECONDS
    )
  }
  
  /**
   * Register health checks for system components
   */
  private def registerHealthChecks(): Unit = {
    // Register RabbitMQ health check
    HealthCheckRegistry.register(
      HealthCheckRegistry.createHealthCheck("rabbitmq", "connection") {
        try {
          val connection = RabbitMQManager.getConnection
          if (connection.isOpen) {
            HealthCheckResult(
              component = "rabbitmq",
              check = "connection",
              status = HealthStatus.Healthy,
              message = "RabbitMQ connection is open"
            )
          } else {
            HealthCheckResult(
              component = "rabbitmq",
              check = "connection",
              status = HealthStatus.Unhealthy,
              message = "RabbitMQ connection is closed"
            )
          }
        } catch {
          case e: Exception =>
            HealthCheckResult(
              component = "rabbitmq",
              check = "connection",
              status = HealthStatus.Unhealthy,
              message = s"Failed to connect to RabbitMQ: ${e.getMessage}"
            )
        }
      }
    )
    
    // Register queue health checks
    MONITORED_QUEUES.foreach { queueName =>
      HealthCheckRegistry.register(
        HealthCheckRegistry.createHealthCheck("rabbitmq", s"queue-$queueName") {
          try {
            val channel = RabbitMQManager.getChannel("system-status")
            val queueDeclare = channel.queueDeclarePassive(queueName)
            val messageCount = queueDeclare.getMessageCount
            
            // Define thresholds for queue depth
            val warningThreshold = 1000 // If more than 1000 messages, it's degraded
            val criticalThreshold = 5000 // If more than 5000 messages, it's unhealthy
            
            val status = messageCount match {
              case count if count > criticalThreshold => HealthStatus.Unhealthy
              case count if count > warningThreshold => HealthStatus.Degraded
              case _ => HealthStatus.Healthy
            }
            
            val statusMessage = status match {
              case HealthStatus.Healthy => s"Queue '$queueName' has $messageCount messages"
              case HealthStatus.Degraded => s"Queue '$queueName' is filling up ($messageCount messages)"
              case HealthStatus.Unhealthy => s"Queue '$queueName' is critically full ($messageCount messages)"
              case _ => s"Queue '$queueName' status unknown"
            }
            
            HealthCheckResult(
              component = "rabbitmq",
              check = s"queue-$queueName",
              status = status,
              message = statusMessage,
              details = Map(
                "messageCount" -> messageCount.toString,
                "consumerCount" -> queueDeclare.getConsumerCount.toString
              )
            )
          } catch {
            case e: Exception =>
              HealthCheckResult(
                component = "rabbitmq",
                check = s"queue-$queueName",
                status = HealthStatus.Unknown,
                message = s"Failed to check queue '$queueName': ${e.getMessage}"
              )
          }
        }
      )
    }
  }
  
  /**
   * Collect queue statistics from RabbitMQ
   */
  private def collectQueueStats(): Unit = {
    try {
      val channel = RabbitMQManager.getChannel("system-status")
      
      MONITORED_QUEUES.foreach { queueName =>
        try {
          val queueDeclare = channel.queueDeclarePassive(queueName)
          val messageCount = queueDeclare.getMessageCount
          val consumerCount = queueDeclare.getConsumerCount
          
          // Update legacy stats
          val existingStats = queueStats.getOrElse(queueName, QueueStats(queueName))
          queueStats.put(queueName, existingStats.copy(
            messageCount = messageCount,
            consumerCount = consumerCount,
            lastUpdated = System.currentTimeMillis()
          ))
          
          // Update metrics in registry
          MetricsRegistry.getGauge(
            MetricsRegistry.MetricMetadata(COMPONENT, "queue", s"$QUEUE_MESSAGE_COUNT", Map("queue" -> queueName))
          ).foreach(_.set(messageCount))
          
          MetricsRegistry.getGauge(
            MetricsRegistry.MetricMetadata(COMPONENT, "queue", s"$QUEUE_CONSUMER_COUNT", Map("queue" -> queueName))
          ).foreach(_.set(consumerCount))
          
        } catch {
          case e: Exception =>
            logger.warn(s"Error getting stats for queue $queueName: ${e.getMessage}")
        }
      }
      
      // Log current stats
      logCurrentStats()
      
    } catch {
      case e: Exception =>
        logger.error("Error collecting queue statistics", e)
    }
  }
  
  /**
   * Log current system status
   */
  private def logCurrentStats(): Unit = {
    val sb = new StringBuilder("Current Queue Stats:\n")
    
    queueStats.values.toList.sortBy(_.name).foreach { stats =>
      sb.append(f"  ${stats.name}%-30s: ${stats.messageCount}%5d messages, ${stats.consumerCount}%2d consumers\n")
    }
    
    logger.info(sb.toString)
  }
  
  /**
   * Update processing time for a specific queue
   */
  def updateProcessingTime(queueName: String, processingTimeMs: Long): Unit = {
    synchronized {
      val existingStats = queueStats.getOrElse(queueName, QueueStats(queueName))
      val newAvg = if (existingStats.avgProcessingTime == 0) {
        processingTimeMs
      } else {
        (existingStats.avgProcessingTime * 9 + processingTimeMs) / 10 // Moving average
      }
      
      queueStats.put(queueName, existingStats.copy(avgProcessingTime = newAvg))
      
      // Update metric in registry
      MetricsRegistry.getHistogram(
        MetricsRegistry.MetricMetadata(COMPONENT, "queue", QUEUE_PROCESSING_TIME, Map("queue" -> queueName))
      ).foreach(_.update(processingTimeMs))
    }
  }
  
  /**
   * Get formatted status report
   */
  def getStatusReport(): String = {
    val sb = new StringBuilder("System Status Report:\n")
    
    queueStats.values.toList.sortBy(_.name).foreach { stats =>
      sb.append(f"  ${stats.name}%-30s: ${stats.messageCount}%5d messages, " +
                f"${stats.consumerCount}%2d consumers, " +
                f"${stats.avgProcessingTime}%4d ms avg processing time\n")
    }
    
    // Add health information
    sb.append("\nHealth Status:\n")
    sb.append(s"  Overall: ${HealthCheckRegistry.getSystemHealth()}\n")
    
    // Add component-specific health
    val componentHealth = Seq("rabbitmq", "database", "recommender", "transformer")
    componentHealth.foreach { comp =>
      sb.append(f"  $comp%-12s: ${HealthCheckRegistry.getComponentHealth(comp)}\n")
    }
    
    sb.toString()
  }
  
  /**
   * Register all metrics during initialization
   */
  def registerMetrics(): Unit = {
    // Register queue metrics
    val queueNames = Seq(
      RabbitMQManager.Queues.PROCESSED_EVENTS,
      RabbitMQManager.Queues.RECOMMENDATIONS,
      RabbitMQManager.Queues.HISTORICAL_DATA
    )
    
    queueNames.foreach { queueName =>
      // Message count gauge
      MetricsRegistry.buildGauge(
        MetricsRegistry.MetricMetadata(COMPONENT, "queue", QUEUE_MESSAGE_COUNT, Map("queue" -> queueName)),
        s"Number of messages in queue $queueName",
        "messages"
      )
      
      // Consumer count gauge
      MetricsRegistry.buildGauge(
        MetricsRegistry.MetricMetadata(COMPONENT, "queue", QUEUE_CONSUMER_COUNT, Map("queue" -> queueName)),
        s"Number of consumers for queue $queueName",
        "consumers"
      )
      
      // Processing time histogram
      MetricsRegistry.buildHistogram(
        MetricsRegistry.MetricMetadata(COMPONENT, "queue", QUEUE_PROCESSING_TIME, Map("queue" -> queueName)),
        s"Processing time for messages in queue $queueName",
        "milliseconds"
      )
    }
    
    // System metrics (CPU, memory, etc. are handled by the MetricsRegistry JVM metrics)
  }
  
  /**
   * Stop monitoring
   */
  def stopMonitoring(): Unit = {
    logger.info("Shutting down system status monitoring")
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

