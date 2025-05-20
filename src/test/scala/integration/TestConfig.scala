package integration

import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.duration._

object TestConfig {
  // Load test-specific configuration
  private val config = ConfigFactory.load("application-test.conf")
  
  // Test timeouts with sensible defaults
  val DefaultTestTimeout: FiniteDuration = 
    Option(System.getProperty("test.timeout.default"))
      .map(d => Duration(d).asInstanceOf[FiniteDuration])
      .getOrElse(10.seconds)
  
  val QueueEmptyTimeout: FiniteDuration = 
    Option(System.getProperty("test.timeout.queueEmpty"))
      .map(d => Duration(d).asInstanceOf[FiniteDuration])
      .getOrElse(5.seconds)
  
  val ConsumerStartTimeout: FiniteDuration = 
    Option(System.getProperty("test.timeout.consumerStart"))
      .map(d => Duration(d).asInstanceOf[FiniteDuration])
      .getOrElse(500.millis)
    
  val MessagePublishDelay: FiniteDuration = 
    Option(System.getProperty("test.delay.messagePublish"))
      .map(d => Duration(d).asInstanceOf[FiniteDuration])
      .getOrElse(50.millis)
    
  val EventBatchSize: Int = 
    Option(System.getProperty("test.performance.eventBatchSize"))
      .map(_.toInt)
      .getOrElse(100)
  
  val MinExpectedEventsPerSecond: Double = 
    Option(System.getProperty("test.performance.minEventsPerSecond"))
      .map(_.toDouble)
      .getOrElse(50.0)
}

