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
import config.RabbitMQManager
import io.circe.syntax._
import io.circe.generic.auto._
import testutil.RabbitMQTestManager

import java.nio.charset.StandardCharsets
import scala.concurrent.ExecutionContext.Implicits.global

import org.powermock.modules.junit4.PowerMockRunner
import org.junit.runner.RunWith

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[RabbitMQManager.type]))
class ConsumerManagerSpec extends AnyFunSuite with RabbitMQTestManager {
  
  var userRepository: UserRepository = _
  var eventRepository: EventRepository = _
  var participationRepository: ParticipationRepository = _
  var friendsRepository: FriendsRepository = _
  var consumerManager: ConsumerManager = _
  
  // Use our RabbitMQTestManager to create a mock channel
  val mockChannel: Channel = createMockChannel("test-channel")

  var mockDelivery: Delivery = _
  var mockEnvelope: Envelope = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    
    userRepository = PowerMockito.mock(classOf[UserRepository])
    eventRepository = PowerMockito.mock(classOf[EventRepository])
    participationRepository = PowerMockito.mock(classOf[ParticipationRepository])
    friendsRepository = PowerMockito.mock(classOf[FriendsRepository])
    mockDelivery = PowerMockito.mock(classOf[Delivery])
    mockEnvelope = PowerMockito.mock(classOf[Envelope])
    
    // Set up expected behavior for common mocks
    val user = User(userId = 1L, name = Some("testuser"), emailAddress = Some("test@example.com"))
    
    when(mockDelivery.getBody).thenReturn(user.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    when(mockDelivery.getEnvelope).thenReturn(mockEnvelope)
    when(RabbitMQManager.createChannel(anyString())).thenReturn(mockChannel)
    when(RabbitMQManager.getChannel(anyString())).thenReturn(mockChannel)
  }

  override def afterEach(): Unit = {
    if (consumerManager != null) {
      consumerManager.close()
      consumerManager = null
    }
    super.afterEach()
  }

  test("User consumer should process messages correctly") {
    val callbackCaptor = ArgumentCaptor.forClass(classOf[DeliverCallback])
    
    when(mockChannel.basicConsume(
      ArgumentMatchers.eq(RabbitMQManager.Queues.USER_INTERACTIONS),
      anyBoolean(),
      any[DeliverCallback],
      any[com.rabbitmq.client.CancelCallback]
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument[DeliverCallback](2)
      "consumerTag"
    })

    consumerManager = new ConsumerManager(userRepository, eventRepository, participationRepository, friendsRepository)
    consumerManager.startAllConsumers()
    
    // Now capture the callback and simulate a message
    verify(mockChannel).basicConsume(
      ArgumentMatchers.eq(RabbitMQManager.Queues.USER_INTERACTIONS),
      anyBoolean(),
      callbackCaptor.capture(),
      any[com.rabbitmq.client.CancelCallback]()
    )
    
    val callback = callbackCaptor.getValue
    callback.handle("consumer-tag", mockDelivery)
    
    // Verify the repository was called with the correct user object
    verify(userRepository, timeout(1000)).upsert(any[User])
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
    
    consumerManager = new ConsumerManager(userRepository, eventRepository, participationRepository, friendsRepository)
    consumerManager.startAllConsumers()
    
    // Manually invoke the callback with our mock delivery
    verify(mockChannel).basicConsume(
      ArgumentMatchers.eq(RabbitMQManager.Queues.EVENT_UPDATES),
      anyBoolean(),
      any[com.rabbitmq.client.DeliverCallback],
      any[com.rabbitmq.client.CancelCallback]
    )
    val callback = callbackCaptor.getValue
    callback.handle("consumer-tag", mockDelivery)
    
    // Verify the repository was called with the correct event object
    verify(eventRepository, timeout(1000)).upsert(ArgumentMatchers.eq(event))
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
    
    consumerManager = new ConsumerManager(userRepository, eventRepository, participationRepository, friendsRepository)
    consumerManager.startAllConsumers()
    
    verify(mockChannel).basicConsume(ArgumentMatchers.eq(RabbitMQManager.Queues.PARTICIPATION_UPDATES), 
                                   anyBoolean(), callbackCaptor.capture(), any[com.rabbitmq.client.CancelCallback]())
    val callback = callbackCaptor.getValue
    callback.handle("consumer-tag", mockDelivery)
    
    verify(participationRepository, timeout(1000)).upsert(ArgumentMatchers.eq(participation))
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
    
    consumerManager = new ConsumerManager(userRepository, eventRepository, participationRepository, friendsRepository)
    consumerManager.startAllConsumers()
    
    verify(mockChannel).basicConsume(ArgumentMatchers.eq(RabbitMQManager.Queues.FRIENDSHIP_UPDATES), 
                                   anyBoolean(), any[com.rabbitmq.client.DeliverCallback], any[com.rabbitmq.client.CancelCallback])
    val callback = callbackCaptor.getValue
    callback.handle("consumer-tag", mockDelivery)
    
    verify(friendsRepository, timeout(1000)).upsert(ArgumentMatchers.eq(friendship))
  }
}