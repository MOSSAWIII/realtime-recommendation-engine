package infrastructure.config

import com.typesafe.config.Config
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Typed configuration for the DataManager component
 */
class DataManagerConfigSection(config: Config) extends ConfigSection {
  private val dataManagerConfig = 
    if (config.hasPath("dataManager")) config.getConfig("dataManager")
    else config.withFallback(defaultDataManagerConfig)
  
  // Database connection settings
  val jdbcUrl: String = config.getString("database.url")
  val username: String = config.getString("database.user")
  val password: String = config.getString("database.password")
  val maxPoolSize: Int = config.getInt("database.poolSize")
  val minIdle: Int = if (config.hasPath("database.minIdle")) config.getInt("database.minIdle") else 2
  val idleTimeout: Long = if (config.hasPath("database.idleTimeout")) 
    Duration(config.getString("database.idleTimeout")).toMillis else 600000L
  val connectionTimeout: Long = if (config.hasPath("database.connectionTimeout")) 
    Duration(config.getString("database.connectionTimeout")).toMillis else 30000L
  val maxLifetime: Long = if (config.hasPath("database.maxLifetime")) 
    Duration(config.getString("database.maxLifetime")).toMillis else 1800000L
  
  // Cache configuration
  val l1CacheSize: Int = dataManagerConfig.getInt("cache.l1Size")
  val l2CacheSize: Int = dataManagerConfig.getInt("cache.l2Size")
  val defaultCacheTTL: FiniteDuration = 
    Duration(dataManagerConfig.getString("cache.defaultTTL")).asInstanceOf[FiniteDuration]
  val shortCacheTTL: FiniteDuration = 
    Duration(dataManagerConfig.getString("cache.shortTTL")).asInstanceOf[FiniteDuration]
  val l2RefreshThreshold: Double = dataManagerConfig.getDouble("cache.l2RefreshThreshold")
  val cacheMaintenanceInterval: FiniteDuration = 
    Duration(dataManagerConfig.getString("cache.maintenanceInterval")).asInstanceOf[FiniteDuration]
  val refreshQueueProcessingInterval: FiniteDuration = 
    Duration(dataManagerConfig.getString("cache.refreshQueueInterval")).asInstanceOf[FiniteDuration]
  
  // Query configuration
  val queryTimeout: FiniteDuration = 
    Duration(dataManagerConfig.getString("query.timeout")).asInstanceOf[FiniteDuration]
  val maxQueryPoolSize: Int = dataManagerConfig.getInt("query.maxPoolSize")
  val defaultBatchSize: Int = dataManagerConfig.getInt("query.defaultBatchSize")
  
  // Transaction configuration
  val transactionTimeout: FiniteDuration = 
    Duration(dataManagerConfig.getString("transaction.timeout")).asInstanceOf[FiniteDuration]
  val maxRetries: Int = dataManagerConfig.getInt("transaction.maxRetries")
  val retryDelay: FiniteDuration = 
    Duration(dataManagerConfig.getString("transaction.retryDelay")).asInstanceOf[FiniteDuration]
  
  override def validate(): Map[String, String] = {
    val errors = scala.collection.mutable.Map[String, String]()
    
    // Cache validation
    if (l1CacheSize <= 0) {
      errors += ("dataManager.cache.l1Size" -> s"L1 cache size must be positive, got $l1CacheSize")
    }
    
    if (l2CacheSize <= 0) {
      errors += ("dataManager.cache.l2Size" -> s"L2 cache size must be positive, got $l2CacheSize")
    }
    
    if (defaultCacheTTL.toMillis <= 0) {
      errors += ("dataManager.cache.defaultTTL" -> s"Default cache TTL must be positive, got $defaultCacheTTL")
    }
    
    if (shortCacheTTL.toMillis <= 0) {
      errors += ("dataManager.cache.shortTTL" -> s"Short cache TTL must be positive, got $shortCacheTTL")
    }
    
    if (l2RefreshThreshold <= 0 || l2RefreshThreshold >= 1) {
      errors += ("dataManager.cache.l2RefreshThreshold" -> 
                s"L2 refresh threshold must be between 0 and 1, got $l2RefreshThreshold")
    }
    
    // Query validation
    if (queryTimeout.toMillis <= 0) {
      errors += ("dataManager.query.timeout" -> s"Query timeout must be positive, got $queryTimeout")
    }
    
    if (maxQueryPoolSize <= 0) {
      errors += ("dataManager.query.maxPoolSize" -> s"Max query pool size must be positive, got $maxQueryPoolSize")
    }
    
    if (defaultBatchSize <= 0) {
      errors += ("dataManager.query.defaultBatchSize" -> s"Default batch size must be positive, got $defaultBatchSize")
    }
    
    // Transaction validation
    if (transactionTimeout.toMillis <= 0) {
      errors += ("dataManager.transaction.timeout" -> s"Transaction timeout must be positive, got $transactionTimeout")
    }
    
    if (maxRetries < 0) {
      errors += ("dataManager.transaction.maxRetries" -> s"Max retries must be non-negative, got $maxRetries")
    }
    
    if (retryDelay.toMillis < 0) {
      errors += ("dataManager.transaction.retryDelay" -> s"Retry delay must be non-negative, got $retryDelay")
    }
    
    errors.toMap
  }
  
  private def defaultDataManagerConfig: Config = {
    import com.typesafe.config.ConfigFactory
    ConfigFactory.parseString(
      """
      dataManager {
        cache {
          l1Size = 10000
          l2Size = 100000
          defaultTTL = "30m"
          shortTTL = "5m"
          l2RefreshThreshold = 0.8
          maintenanceInterval = "1m"
          refreshQueueInterval = "10s"
        }
        query {
          timeout = "30s"
          maxPoolSize = 20
          defaultBatchSize = 100
        }
        transaction {
          timeout = "30s"
          maxRetries = 3
          retryDelay = "1s"
        }
      }
      """
    )
  }
}

