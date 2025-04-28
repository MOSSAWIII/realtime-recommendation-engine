package unit.core

import com.rabbitmq.client.{Channel, DeliverCallback, Delivery, Envelope}
import core.ConsumerManager
import models.{User, Event, EventParticipation, Friendship}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyString}
import org.mockito.Mockito.{timeout, verify, when}
import org.powermock.api.mockito.PowerMockito
import org.powermock.core.classloader.annotations.PrepareForTest
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import repositories.{UserRepository, EventRepository, ParticipationRepository, FriendsRepository}
import config.RabbitMQConfig
import io.circe.syntax._
import io.circe.generic.auto._
import testutil.TestImports // Import the new helper

import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext.Implicits.global

@PrepareForTest(Array(classOf[config.RabbitMQConfig$]))
class ConsumerManagerSpec extends AnyFunSuite with BeforeAndAfterEach {

  var mockUserRepo: UserRepository = _
  var mockEventRepo: EventRepository = _
  var mockParticipationRepo: ParticipationRepository = _
  var mockFriendsRepo: FriendsRepository = _
  var mockDelivery: Delivery = _
  var mockEnvelope: Envelope = _

  // Use the helper method
  val mockChannel: Channel = TestImports.mockRabbitMQConfig()

  override def beforeEach(): Unit = {
    PowerMockito.mockStatic(classOf[config.RabbitMQConfig])
    
    mockUserRepo = PowerMockito.mock(classOf[UserRepository])
    mockEventRepo = PowerMockito.mock(classOf[EventRepository])
    mockParticipationRepo = PowerMockito.mock(classOf[ParticipationRepository])
    mockFriendsRepo = PowerMockito.mock(classOf[FriendsRepository])
    mockDelivery = PowerMockito.mock(classOf[Delivery])
    mockEnvelope = PowerMockito.mock(classOf[Envelope])
    
    val user = User(userId = 1L, name = Some("testuser"), emailAddress = Some("test@example.com"))
    
    when(mockDelivery.getBody).thenReturn(user.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    when(mockDelivery.getEnvelope).thenReturn(mockEnvelope)
    when(config.RabbitMQConfig.createChannel()).thenReturn(mockChannel)
  }

  test("User consumer should process messages correctly") {
    when(mockChannel.basicConsume(
      ArgumentMatchers.eq(config.RabbitMQConfig.Queues.USER_INTERACTIONS),
      ArgumentMatchers.eq(false),
      any[DeliverCallback],
      any[com.rabbitmq.client.CancelCallback]
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument[DeliverCallback](2)
      callback.handle("consumer-tag", mockDelivery)
      "consumerTag"
    })

    val consumerManager = new ConsumerManager(mockUserRepo, mockEventRepo, mockParticipationRepo, mockFriendsRepo)
    consumerManager.startAllConsumers()
    
    // Verify the repository was called with the correct user object
    verify(mockUserRepo, timeout(1000)).upsert(ArgumentMatchers.eq(
      User(userId = 1L, name = Some("testuser"), emailAddress = Some("test@example.com"))
    ))
  }
  
  test("Event consumer should process messages correctly") {
    val event = Event(id = Some(1L), name = "Test Event", description = Some("Test Event Description"), 
      startTime = System.currentTimeMillis(), endTime = Some(System.currentTimeMillis() + 3600000), 
      location = Some("Test Location"), createdBy = 1L, status = "active")
    
    when(mockDelivery.getBody).thenReturn(event.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    when(mockDelivery.getEnvelope).thenReturn(mockEnvelope)
    
    val callbackCaptor = ArgumentCaptor.forClass(classOf[DeliverCallback])
    
    when(mockChannel.basicConsume(
      anyString(),
      anyBoolean(),
      callbackCaptor.capture(),
      any[com.rabbitmq.client.CancelCallback]
    )).thenReturn("consumerTag")
    
    val consumerManager = new ConsumerManager(mockUserRepo, mockEventRepo, mockParticipationRepo, mockFriendsRepo)
    consumerManager.startAllConsumers()
    
    // Manually invoke the callback with our mock delivery
    verify(mockChannel).basicConsume(
      ArgumentMatchers.eq(config.RabbitMQConfig.Queues.EVENT_UPDATES),
      anyBoolean(),
      any[com.rabbitmq.client.DeliverCallback],
      any[com.rabbitmq.client.CancelCallback]
    )
    val callback = callbackCaptor.getValue
    callback.handle("consumer-tag", mockDelivery)
    
    // Verify the repository was called with the correct event object
    verify(mockEventRepo, timeout(1000)).upsert(ArgumentMatchers.eq(event))
  }

  test("Participation consumer should process messages correctly") {
    val participation = EventParticipation(id = Some(1L), userId = 1L, eventId = 1L, status = "attending")
    
    when(mockDelivery.getBody).thenReturn(participation.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    when(mockDelivery.getEnvelope).thenReturn(mockEnvelope)
    
    val callbackCaptor = ArgumentCaptor.forClass(classOf[DeliverCallback])
    
    when(mockChannel.basicConsume(
      anyString(), 
      anyBoolean(), 
      any[DeliverCallback],
      any[com.rabbitmq.client.CancelCallback]
    )).thenReturn("consumerTag")
    
    val consumerManager = new ConsumerManager(mockUserRepo, mockEventRepo, mockParticipationRepo, mockFriendsRepo)
    consumerManager.startAllConsumers()
    
    verify(mockChannel).basicConsume(ArgumentMatchers.eq(config.RabbitMQConfig.Queues.PARTICIPATION_UPDATES), 
                                   anyBoolean(), callbackCaptor.capture(), any[com.rabbitmq.client.CancelCallback]())
    val callback = callbackCaptor.getValue
    callback.handle("consumer-tag", mockDelivery)
    
    verify(mockParticipationRepo, timeout(1000)).upsert(ArgumentMatchers.eq(participation))
  }

  test("Friendship consumer should process messages correctly") {
    val friendship = Friendship(id = Some(1L), userId1 = 1L, userId2 = 2L, status = "accepted", createdAt = System.currentTimeMillis())
    
    when(mockDelivery.getBody).thenReturn(friendship.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    when(mockDelivery.getEnvelope).thenReturn(mockEnvelope)
    
    val callbackCaptor = ArgumentCaptor.forClass(classOf[DeliverCallback])
    
    when(mockChannel.basicConsume(
      anyString(), 
      anyBoolean(), 
      any[DeliverCallback],
      any[com.rabbitmq.client.CancelCallback]
    )).thenReturn("consumerTag")
    
    val consumerManager = new ConsumerManager(mockUserRepo, mockEventRepo, mockParticipationRepo, mockFriendsRepo)
    consumerManager.startAllConsumers()
    
    verify(mockChannel).basicConsume(ArgumentMatchers.eq(config.RabbitMQConfig.Queues.FRIENDSHIP_UPDATES), 
                                   anyBoolean(), any[com.rabbitmq.client.DeliverCallback], any[com.rabbitmq.client.CancelCallback])
    val callback = callbackCaptor.getValue
    callback.handle("consumer-tag", mockDelivery)
    
    verify(mockFriendsRepo, timeout(1000)).upsert(ArgumentMatchers.eq(friendship))
  }
}