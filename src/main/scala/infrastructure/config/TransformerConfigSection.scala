package infrastructure.config

import com.typesafe.config.Config
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Typed configuration for the Transformer component
 */
class TransformerConfigSection(config: Config) extends ConfigSection {
  private val transformerConfig = 
    if (config.hasPath("transformer")) config.getConfig("transformer")
    else config.withFallback(defaultTransformerConfig)
  
  // Channel configuration
  val channelPrefetchCount: Int = transformerConfig.getInt("channel.prefetchCount")
  
  // Processing configuration
  val maxProcessingRetries: Int = transformerConfig.getInt("processing.maxRetries")
  val processingRetryDelay: FiniteDuration = 
    Duration(transformerConfig.getString("processing.retryDelay")).asInstanceOf[FiniteDuration]
  
  // Publisher confirms
  val publisherConfirmTimeout: FiniteDuration = 
    Duration(transformerConfig.getString("publisherConfirm.timeout")).asInstanceOf[FiniteDuration]
  
  // Exchange and routing settings
  val exchangeName: String = transformerConfig.getString("exchange.name")
  val processedEventsRoutingTemplate: String = transformerConfig.getString("routing.processedEventsTemplate")
  val recommendationRequestRoutingTemplate: String = transformerConfig.getString("routing.recommendationRequestTemplate")
  
  // Metrics configuration
  val metricResetInterval: FiniteDuration = 
    Duration(transformerConfig.getString("metrics.resetInterval")).asInstanceOf[FiniteDuration]
  
  override def validate(): Map[String, String] = {
    val errors = scala.collection.mutable.Map[String, String]()
    
    if (channelPrefetchCount <= 0) {
      errors += ("transformer.channel.prefetchCount" -> s"Channel prefetch count must be positive, got $channelPrefetchCount")
    }
    
    if (maxProcessingRetries < 0) {
      errors += ("transformer.processing.maxRetries" -> s"Max processing retries must be non-negative, got $maxProcessingRetries")
    }
    
    if (processingRetryDelay.toMillis < 0) {
      errors += ("transformer.processing.retryDelay" -> s"Processing retry delay must be non-negative, got $processingRetryDelay")
    }
    
    if (publisherConfirmTimeout.toMillis <= 0) {
      errors += ("transformer.publisherConfirm.timeout" -> s"Publisher confirm timeout must be positive, got $publisherConfirmTimeout")
    }
    
    if (exchangeName.isEmpty) {
      errors += ("transformer.exchange.name" -> "Exchange name cannot be empty")
    }
    
    if (!processedEventsRoutingTemplate.contains("#")) {
      errors += ("transformer.routing.processedEventsTemplate" -> 
                s"Processed events routing template must contain # wildcard, got $processedEventsRoutingTemplate")
    }
    
    if (!recommendationRequestRoutingTemplate.contains("#")) {
      errors += ("transformer.routing.recommendationRequestTemplate" -> 
                s"Recommendation request routing template must contain # wildcard, got $recommendationRequestRoutingTemplate")
    }
    
    errors.toMap
  }
  
  private def defaultTransformerConfig: Config = {
    import com.typesafe.config.ConfigFactory
    ConfigFactory.parseString(
      """
      transformer {
        channel {
          prefetchCount = 10
        }
        processing {
          maxRetries = 3
          retryDelay = "5s"
        }
        publisherConfirm {
          timeout = "30s"
        }
        exchange {
          name = "user_actions_exchange"
        }
        routing {
          processedEventsTemplate = "event.processed.#"
          recommendationRequestTemplate = "recommendation.request.#"
        }
        metrics {
          resetInterval = "10m"
        }
      }
      """
    )
  }
}

