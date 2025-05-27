package infrastructure.config

import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import javax.management.{MBeanServer, ObjectName}
import java.lang.management.ManagementFactory
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import java.util.{List => JList, Map => JMap, Set => JSet}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import org.slf4j.LoggerFactory
import scala.reflect.ClassTag

/**
 * Centralized configuration service that provides:
 * - Typed configuration access with validation
 * - Hot reloading capabilities
 * - Configuration change notifications
 * - JMX management operations
 */
class ConfigurationService(initialConfig: Option[Config] = None) extends ConfigurationServiceInterface {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Current loaded configuration
  @volatile private var currentConfig = initialConfig.getOrElse(ConfigFactory.load())
  
  // Scheduler for periodic config reload
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  
  // Default validation errors container
  private val validationErrors = new ConcurrentHashMap[String, String]()
  
  // Change listeners registry
  private val changeListeners = new mutable.HashMap[String, mutable.Set[ConfigChangeListener]]() 
    with mutable.MultiMap[String, ConfigChangeListener]
  
  // Configuration section cache
  private val configSections = new ConcurrentHashMap[String, AnyRef]()
  
  /**
   * Initialize the configuration service
   */
  def initialize(): Unit = {
    logger.info("Initializing ConfigurationService")
    registerMBean()
    validateAllConfigurations()
    scheduleConfigReload()
  }
  
  /**
   * Get a typed configuration section
   *
   * @param factory Factory function to create the typed config section
   * @tparam T The type of the configuration section
   * @return The typed configuration section
   */
  def getConfigSection[T <: ConfigSection](factory: Config => T): T = {
    val sectionName = factory.getClass.getSimpleName
    
    configSections.computeIfAbsent(
      sectionName, 
      _ => factory(currentConfig)
    ).asInstanceOf[T]
  }

  /**
   * Récupère une section de configuration typée
   */
  def get[T: ClassTag]: T = {
    val tag = implicitly[ClassTag[T]]
    val constructor = tag.runtimeClass.getConstructors.head
    
    try {
      constructor.newInstance(currentConfig).asInstanceOf[T]
    } catch {
      case e: Exception => 
        throw new RuntimeException(s"Failed to instantiate configuration section ${tag.runtimeClass.getName}: ${e.getMessage}", e)
    }
  }

  /**
   * Get a typed configuration section using implicit converter
   */
  override def get[T <: ConfigSection](implicit converter: Config => T): T = {
    val sectionName = converter.getClass.getSimpleName
    configSections.computeIfAbsent(
      sectionName,
      _ => converter(currentConfig)
    ).asInstanceOf[T]
  }

  /**
   * Get raw configuration
   */
  override def getConfig: Config = currentConfig

  /**
   * Reload configuration from disk with public interface
   */
  override def reload(): Boolean = reloadConfig()

  /**
   * Update configuration value (string version)
   */
  override def updateConfigValue(path: String, value: String): Boolean = {
    updateConfigValueGeneric(path, value)
  }

  /**
   * Update a configuration value with any type
   */
  override def updateConfigValueGeneric(path: String, value: Any): Boolean = {
    try {
      val newValue = ConfigValueFactory.fromAnyRef(value)
      val newConfig = currentConfig.withValue(path, newValue)
      updateConfiguration(newConfig)
      notifyChangeListeners(newConfig)
      validateAllConfigurations()
      true
    } catch {
      case e: Exception =>
        logger.error(s"Failed to update configuration value at path $path", e)
        false
    }
  }

  /**
   * Reload configuration from disk
   *
   * @return True if reload was successful and configuration changed
   */
  def reloadConfig(): Boolean = {
    logger.info("Reloading configuration")
    
    try {
      val newConfig = ConfigFactory.load()
      
      if (configChanged(currentConfig, newConfig)) {
        updateConfiguration(newConfig)
        notifyChangeListeners(newConfig)
        invalidateConfigSections()
        validateAllConfigurations()
        logger.info("Configuration reloaded successfully with changes detected")
        true
      } else {
        logger.info("Configuration reloaded, no changes detected")
        false
      }
    } catch {
      case ex: Exception =>
        logger.error("Failed to reload configuration", ex)
        false
    }
  }
  
  /**
   * Update a specific configuration path
   *
   * @param path Path to update
   * @param value New value
   * @return True if update was successful
   */
  def updateConfigValue(path: String, value: Any): Boolean = {
    try {
      val configValue = ConfigValueFactory.fromAnyRef(value)
      val updatedConfig = currentConfig.withValue(path, configValue)
      updateConfiguration(updatedConfig)
      notifyPathChangeListeners(path, configValue)
      invalidateConfigSections()
      validateAllConfigurations()
      logger.info(s"Configuration value updated: $path = $value")
      true
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to update configuration value: $path", ex)
        false
    }
  }
  
  /**
   * Register a listener for configuration changes
   *
   * @param path Configuration path to monitor (or "" for all changes)
   * @param listener Listener to notify
   */
  def addChangeListener(path: String, listener: ConfigChangeListener): Unit = {
    changeListeners.addBinding(path, listener)
    logger.debug(s"Added configuration change listener for path: $path")
  }
  
  /**
   * Remove a change listener
   *
   * @param path Configuration path
   * @param listener Listener to remove
   */
  def removeChangeListener(path: String, listener: ConfigChangeListener): Unit = {
    changeListeners.removeBinding(path, listener)
    logger.debug(s"Removed configuration change listener for path: $path")
  }
  
  /**
   * Get current validation errors
   *
   * @return Map of path to error message
   */
  def getValidationErrors: Map[String, String] = {
    validationErrors.asScala.toMap
  }
  
  /**
   * Check if configuration is valid
   *
   * @return True if configuration is valid
   */
  def isConfigValid: Boolean = {
    validationErrors.isEmpty
  }
  
  /**
   * Get raw configuration
   */
  def getRawConfig: Config = currentConfig
  
  /**
   * Shut down the configuration service
   */
  def shutdown(): Unit = {
    logger.info("Shutting down ConfigurationService")
    unregisterMBean()
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
  
  // Private methods
  
  private def updateConfiguration(newConfig: Config): Unit = {
    currentConfig = newConfig
  }
  
  private def invalidateConfigSections(): Unit = {
    configSections.clear()
  }
  
  private def configChanged(oldConfig: Config, newConfig: Config): Boolean = {
    !oldConfig.equals(newConfig)
  }
  
  private def notifyChangeListeners(newConfig: Config): Unit = {
    // Notify global listeners first
    val globalListeners = changeListeners.getOrElse("", mutable.Set.empty)
    globalListeners.foreach(_.onConfigChanged(newConfig))
    
    // Notify path-specific listeners
    val allPaths = changeListeners.keySet - ""
    allPaths.foreach { path =>
      if (newConfig.hasPath(path) && currentConfig.hasPath(path) &&
          !newConfig.getValue(path).equals(currentConfig.getValue(path))) {
        val listeners = changeListeners.getOrElse(path, mutable.Set.empty)
        val value = newConfig.getValue(path)
        listeners.foreach(_.onConfigValueChanged(path, value))
      }
    }
  }
  
  private def notifyPathChangeListeners(path: String, value: ConfigValue): Unit = {
    // Notify global listeners
    val globalListeners = changeListeners.getOrElse("", mutable.Set.empty)
    globalListeners.foreach(_.onConfigChanged(currentConfig))
    
    // Notify specific path listeners
    val listeners = changeListeners.getOrElse(path, mutable.Set.empty)
    listeners.foreach(_.onConfigValueChanged(path, value))
  }
  
  private def validateAllConfigurations(): Unit = {
    validationErrors.clear()
    
    // Run all registered validations for each section
    configSections.values().asScala.foreach {
      case section: ConfigSection =>
        try {
          section.validate().foreach { case (path, error) =>
            validationErrors.put(path, error)
          }
        } catch {
          case ex: Exception =>
            val sectionName = section.getClass.getSimpleName
            validationErrors.put(s"$sectionName.validation", ex.getMessage)
            logger.error(s"Error validating section $sectionName", ex)
        }
      case _ => // Ignore non-ConfigSection objects
    }
    
    // Log validation status
    if (validationErrors.isEmpty) {
      logger.info("All configuration sections validated successfully")
    } else {
      logger.error(s"Configuration validation failed with ${validationErrors.size()} errors:")
      validationErrors.asScala.foreach { case (path, error) =>
        logger.error(s"  $path: $error")
      }
    }
  }
  
  private def scheduleConfigReload(): Unit = {
    val reloadIntervalSeconds = currentConfig.getInt("config.reloadIntervalSeconds")
    logger.info(s"Scheduling configuration reload every $reloadIntervalSeconds seconds")
    
    scheduler.scheduleAtFixedRate(
      () => Try(reloadConfig()),
      reloadIntervalSeconds,
      reloadIntervalSeconds,
      TimeUnit.SECONDS
    )
  }
  
  private def registerMBean(): Unit = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer
      val mbeanName = new ObjectName("com.recommendation.config:type=ConfigurationService")
      mbs.registerMBean(new ConfigurationServiceMBean(this), mbeanName)
      logger.info("Registered ConfigurationService MBean")
    } catch {
      case ex: Exception =>
        logger.warn("Failed to register ConfigurationService MBean", ex)
    }
  }
  
  private def unregisterMBean(): Unit = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer
      val mbeanName = new ObjectName("com.recommendation.config:type=ConfigurationService")
      if (mbs.isRegistered(mbeanName)) {
        mbs.unregisterMBean(mbeanName)
        logger.info("Unregistered ConfigurationService MBean")
      }
    } catch {
      case ex: Exception =>
        logger.warn("Failed to unregister ConfigurationService MBean", ex)
    }
  }
}

/**
 * Base trait for all configuration sections
 */
trait ConfigSection {
  /**
   * Validate the configuration section
   *
   * @return Map of path to error message, empty if valid
   */
  def validate(): Map[String, String]
}

/**
 * Interface for configuration change listeners
 */
trait ConfigChangeListener {
  /**
   * Called when the entire configuration changes
   *
   * @param newConfig The new configuration
   */
  def onConfigChanged(newConfig: Config): Unit = {}
  
  /**
   * Called when a specific configuration value changes
   *
   * @param path Path of the changed value
   * @param value New value
   */
  def onConfigValueChanged(path: String, value: ConfigValue): Unit = {}
}

/**
 * JMX interface for ConfigurationService
 */
class ConfigurationServiceMBean(service: ConfigurationService) extends ConfigurationServiceMXBean {
  override def reloadConfiguration(): Boolean = service.reloadConfig()
  
  override def isConfigurationValid(): Boolean = service.isConfigValid
  
  override def getValidationErrors(): JMap[String, String] = {
    service.getValidationErrors.asJava
  }
  
  override def updateConfigValue(path: String, value: String): Boolean = {
    service.updateConfigValueGeneric(path, value)
  }
}

/**
 * JMX bean interface for ConfigurationService
 */
trait ConfigurationServiceMXBean {
  def reloadConfiguration(): Boolean
  def isConfigurationValid(): Boolean
  def getValidationErrors(): JMap[String, String]
  def updateConfigValue(path: String, value: String): Boolean
}

/**
 * Companion object for ConfigurationService with singleton instance
 */
object ConfigurationService {
  private val logger = LoggerFactory.getLogger(getClass)
  private var instance: Option[ConfigurationService] = None
  
  /**
   * Initialize the configuration service with the given config
   *
   * @param config Optional initial configuration
   */
  def initialize(config: Option[Config] = None): Unit = synchronized {
    if (instance.isEmpty) {
      val service = config match {
        case Some(c) => new ConfigurationService(Some(c))
        case None => new ConfigurationService()
      }
      service.initialize()
      instance = Some(service)
      logger.info("ConfigurationService singleton initialized")
    } else {
      logger.warn("ConfigurationService already initialized")
    }
  }
  
  /**
   * Get the ConfigurationService instance
   *
   * @return The singleton instance
   */
  def getInstance: ConfigurationService = synchronized {
    instance.getOrElse {
      logger.info("Initializing ConfigurationService on first access")
      initialize()
      instance.get
    }
  }
  
  // Keep the factory method for backward compatibility
}

