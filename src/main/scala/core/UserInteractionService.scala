package core

import models._
import repositories.InteractionRepository
import config.RabbitMQConfig
import scala.concurrent.{ExecutionContext, Future}
import java.time.LocalDateTime
import com.rabbitmq.client.AMQP.BasicProperties
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import com.rabbitmq.client.{DeliverCallback, CancelCallback}

class UserInteractionService(
  interactionRepo: InteractionRepository,
  cacheManager: CacheManager
)(implicit ec: ExecutionContext) {
  
  private val channel = RabbitMQConfig.createChannel()
  private val exchangeName = RabbitMQConfig.Exchanges.USER_INTERACTIONS
  
  // Set up the consumer for processing interactions
  def startConsumer(): Unit = {
    val queueName = RabbitMQConfig.Queues.USER_INTERACTIONS
    
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        io.circe.parser.decode[UserInteraction](message) match {
          case Right(interaction) =>
            // Process the interaction
            interactionRepo.save(interaction)
              .map(_ => {
                // Invalidate cached recommendations for this user
                cacheManager.invalidateUserRecommendations(interaction.userId)
                println(s"Processed interaction: ${interaction.userId} -> ${interaction.eventId}")
              })
          case Left(error) =>
            println(s"Error decoding interaction message: ${error.getMessage}")
        }
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    
    channel.basicConsume(queueName, false, deliverCallback, 
      consumerTag => println(s"Interaction consumer cancelled: $consumerTag"))
  }
  
  // Record a user interaction by publishing to message queue
  def recordInteraction(userId: Long, eventId: Long, status: String): Future[Boolean] = {
    val interaction = UserInteraction(
      userId = userId,
      eventId = eventId,
      status = status
    )
    
    Future {
      try {
        val routingKey = s"interaction.$status"
        val props = new BasicProperties.Builder()
          .contentType("application/json")
          .build()
        
        channel.basicPublish(
          exchangeName,
          routingKey,
          props,
          interaction.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
        )
        
        println(s"Published interaction: user $userId with event $eventId ($status)")
        true
      } catch {
        case e: Exception =>
          println(s"Error publishing interaction: ${e.getMessage}")
          false
      }
    }
  }
  
  // Method for batch interactions
  def recordBatchInteractions(interactions: List[UserInteraction]): Future[Boolean] = {
    Future.sequence(
      interactions.map(interaction => {
        val routingKey = s"interaction.${interaction.status}"
        Future {
          try {
            val props = new BasicProperties.Builder()
              .contentType("application/json")
              .build()
            
            channel.basicPublish(
              exchangeName,
              routingKey,
              props,
              interaction.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
            )
            true
          } catch {
            case e: Exception =>
              println(s"Error publishing interaction for user ${interaction.userId}: ${e.getMessage}")
              false
          }
        }
      })
    ).map(results => results.forall(identity))
  }
  
  def close(): Unit = {
    if (channel.isOpen) {
      channel.close()
    }
  }
}