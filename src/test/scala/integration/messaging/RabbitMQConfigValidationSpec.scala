package integration.messaging

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.rabbitmq.client.{ConnectionFactory, Channel}
import infrastructure.messaging.rabbitmq.RabbitMQManager
import infrastructure.config.ConfigurationService
import common.base.RabbitMQTestBase
import scala.util.{Try, Success, Failure}

class RabbitMQConfigValidationSpec extends AnyFlatSpec with Matchers with RabbitMQTestBase {

  "RabbitMQ Configuration" should "match production setup in test environment" in {
    val channel = RabbitMQManager.createChannel("config-validation")
    
    try {
      // Vérifier que tous les exchanges existent
      validateExchangeExists(channel, "user_actions_exchange", "topic")
      validateExchangeExists(channel, "dlx", "direct")
      
      // Vérifier que toutes les queues existent avec les bonnes propriétés
      validateQueueExists(channel, "processed_events_queue")
      validateQueueExists(channel, "recommendations_queue")
      validateQueueExists(channel, "historical_data_queue")
      validateQueueExists(channel, "dead_letter_queue")
      
      // Vérifier les bindings
      validateBindings(channel)
      
    } finally {
      channel.close()
    }
  }

  // Corriger la syntaxe ScalaTest
  "RabbitMQ Configuration" should "support dynamic routing keys with userId" in {
    val channel = RabbitMQManager.createChannel("dynamic-routing-test")
    val testUserId = 12345L
    
    try {
      // Test routing key dynamique pour recommendations
      val dynamicRoutingKey = s"recommendation.result.${testUserId}"
      
      // Publier un message avec routing key dynamique
      val testMessage = """{"userId":12345,"recommendations":[]}"""
      channel.basicPublish(
        "user_actions_exchange",
        dynamicRoutingKey,
        null,
        testMessage.getBytes()
      )
      
      // Vérifier que le message est routé correctement
      val response = channel.basicGet("recommendations_queue", true)
      response should not be null
      new String(response.getBody) shouldBe testMessage
      
    } finally {
      channel.close()
    }
  }

  private def validateExchangeExists(channel: Channel, exchangeName: String, exchangeType: String): Unit = {
    Try {
      // Tentative de déclarer l'exchange (idempotent si il existe déjà)
      channel.exchangeDeclarePassive(exchangeName)
    } match {
      case Success(_) => 
        println(s"✓ Exchange '$exchangeName' exists")
      case Failure(exception) => 
        fail(s"Exchange '$exchangeName' does not exist: ${exception.getMessage}")
    }
  }

  private def validateQueueExists(channel: Channel, queueName: String): Unit = {
    Try {
      channel.queueDeclarePassive(queueName)
    } match {
      case Success(result) => 
        println(s"✓ Queue '$queueName' exists with ${result.getMessageCount} messages")
      case Failure(exception) => 
        fail(s"Queue '$queueName' does not exist: ${exception.getMessage}")
    }
  }

  private def validateBindings(channel: Channel): Unit = {
    // Tester les bindings en publiant des messages avec différents routing keys
    val testCases = Map(
      "user.actions.view" -> "processed_events_queue",
      "event.processed.12345" -> "processed_events_queue", 
      "recommendation.result.12345" -> "recommendations_queue",
      "recommendation.request.12345" -> "recommendations_queue",
      "historical.data.archive" -> "historical_data_queue"
    )

    testCases.foreach { case (routingKey, expectedQueue) =>
      val testMessage = s"""{"test":"binding","routingKey":"$routingKey"}"""
      
      // Publier le message
      channel.basicPublish("user_actions_exchange", routingKey, null, testMessage.getBytes())
      
      // Vérifier qu'il arrive dans la bonne queue
      val response = channel.basicGet(expectedQueue, true)
      response should not be null
      new String(response.getBody) should include(routingKey)
      
      println(s"✓ Binding: $routingKey → $expectedQueue")
    }
  }
}

