package common.utils

import infrastructure.messaging.rabbitmq.RabbitMQManager
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

object RabbitMQConfigValidator extends Matchers {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Validate that test configuration matches production configuration
   */
  def validateConfiguration(): Unit = {
    logger.info("Validating RabbitMQ configuration consistency...")
    
    // Validate exchange names
    val productionExchange = RabbitMQManager.Exchanges.USER_ACTIONS
    productionExchange shouldBe "user_actions_exchange"
    
    // Validate queue names
    val processedQueue = RabbitMQManager.Queues.PROCESSED_EVENTS
    val recommendationsQueue = RabbitMQManager.Queues.RECOMMENDATIONS
    val historicalQueue = RabbitMQManager.Queues.HISTORICAL_DATA
    
    processedQueue shouldBe "processed_events_queue"
    recommendationsQueue shouldBe "recommendations_queue"
    historicalQueue shouldBe "historical_data_queue"
    
    // Validate routing keys
    val userActionsKey = RabbitMQManager.RoutingKeys.USER_ACTIONS
    val processedEventsKey = RabbitMQManager.RoutingKeys.PROCESSED_EVENTS
    
    userActionsKey shouldBe "user.actions.#"
    processedEventsKey shouldBe "event.processed.#"
    
    logger.info("RabbitMQ configuration validation passed!")
  }
  
  /**
   * Validate bindings exist and are correct
   */
  def validateBindings(expectedBindings: Map[String, List[String]]): Unit = {
    expectedBindings.foreach { case (queue, routingKeys) =>
      logger.debug(s"Validating bindings for queue: $queue")
      routingKeys.foreach { routingKey =>
        logger.debug(s"  - Routing key: $routingKey")
      }
    }
    
    // Expected bindings based on production
    val expectedProductionBindings = Map(
      "processed_events_queue" -> List("user.actions.#", "event.processed.#"),
      "recommendations_queue" -> List("recommendation.result.#"),
      "historical_data_queue" -> List("historical.data.#")
    )
    
    expectedBindings shouldBe expectedProductionBindings
    logger.info("Binding validation passed!")
  }
}

