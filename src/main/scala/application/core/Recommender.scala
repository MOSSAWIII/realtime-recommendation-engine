package application.core

import domain.models._
import infrastructure.messaging.rabbitmq.RabbitMQManager
import com.rabbitmq.client.{DeliverCallback, CancelCallback, AMQP}
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._
import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future, Await, Promise, TimeoutException}
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.util.{Success, Failure, Try}
import java.util.LinkedHashMap
import java.util.function.{Function => JFunction, BiFunction => JBiFunction}
import scala.jdk.CollectionConverters._
import utils.metrics.{MetricsRegistry, HealthCheckRegistry, SystemStatus, HealthStatus, HealthCheckResult}
import application.core.RecommenderConstants._

/**
 * Enhanced recommender component that generates personalized recommendations
 * using in-memory caching and optimized algorithms.
 */
class Recommender(private val dataManager: DataManager)(implicit val ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val componentName = "recommender"
  
  // Define metrics
  private val requestTimer = MetricsRegistry.buildTimer(
    MetricsRegistry.MetricMetadata(componentName, "requests", "duration"),
    "Duration of recommendation requests",
    "milliseconds"
  )
  
  private val recommendationsCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "recommendations", "total"),
    "Total number of recommendations generated"
  )
  
  private val cacheHitRatio = MetricsRegistry.buildGauge(
    MetricsRegistry.MetricMetadata(componentName, "cache", "hit_ratio"),
    "Cache hit ratio percentage",
    "percent"
  )
  
  // Define histograms for tracking score distributions
  private val scoreHistogram = MetricsRegistry.buildHistogram(
    MetricsRegistry.MetricMetadata(componentName, "recommendations", "scores"),
    "Distribution of recommendation scores",
    "score"
  )
  
  // Register health checks
  private def registerHealthChecks(): Unit = {
    HealthCheckRegistry.register(
      HealthCheckRegistry.createHealthCheck(componentName, "service") {
        HealthCheckResult(
          component = componentName,
          check = "service",
          status = if (circuitOpen.get()) HealthStatus.Degraded else HealthStatus.Healthy,
          message = if (circuitOpen.get()) 
            "Recommender circuit breaker is open" 
          else 
            "Recommender service is operational",
          details = Map(
            "cacheSize" -> userProfileCache.size.toString,
            "eventCacheSize" -> eventCache.size.toString,
            "avgResponseTime" -> requestTimer.getRecentAvg().toString
          )
        )
      }
    )
  }
  
  // Consolider les canaux:

  // Canaux RabbitMQ consolidés
  private var channelConsumer = RabbitMQManager.createChannel("recommender-channel")
  private var channelPublisher = RabbitMQManager.createChannel("recommender-publisher")

  // Time constants
  private val DayInMillis = 86400000L
  private val WeekInMillis = 7 * DayInMillis
  
  
  // Memory caches
  private class LRUCache[K, V](maxSize: Int) {
    private val map = new LinkedHashMap[K, V](16, 0.75f, true) {
      override protected def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = {
        this.size() > maxSize
      }
    }
    
    def get(key: K): Option[V] = Option(map.get(key))
    def put(key: K, value: V): V = { map.put(key, value); value }
    def clear(): Unit = map.clear()
    def size: Int = map.size()
    def contains(key: K): Boolean = map.containsKey(key)
    def entries: List[(K, V)] = map.entrySet().asScala.map(e => (e.getKey, e.getValue)).toList
  }
  
  // In-memory data structures
  private val recentUserEvents = new TrieMap[Long, mutable.Queue[ProcessedEvent]]()
  private val userProfileCache = new LRUCache[Long, User](1000)
  private val eventCache = new TrieMap[Long, Event]()
  private val userFriendsCache = new TrieMap[Long, List[UserFriend]]()
  private val similarityMatrixCache = new TrieMap[(Long, Long), Double]() // item-item similarity
  
  // Cache for recent recommendations to avoid duplicate processing
  private val recentRecommendations = new TrieMap[Long, (Recommendation, Long)]() // userId -> (recommendation, timestamp)
  private val recommendationCacheTTL = 30.seconds.toMillis
  
  // Performance metrics
  private val totalRequests = new AtomicLong(0)
  private val successfulRequests = new AtomicLong(0)
  private val failedRequests = new AtomicLong(0)
  private val totalResponseTime = new AtomicLong(0)
  private val avgResponseTime = new AtomicLong(0)
  private val minResponseTime = new AtomicLong(Long.MaxValue)
  private val maxResponseTime = new AtomicLong(0)
  
  // Sliding window for metrics calculation
  private val recentResponseTimes = new mutable.Queue[Long]()
  private val maxSlidingWindowSize = 100
  private val metricsLock = new Object()
  
  // Circuit breaker
  private val circuitOpen = new AtomicBoolean(false)
  private val failureThreshold = 5
  private val resetTimeout = 30.seconds.toMillis
  private var lastFailureTime = 0L
  private var consecutiveFailures = 0
  
  // Scheduler for background tasks
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(2)

  /**
   * Starts the recommender service by initializing all message consumers
   * and scheduling background tasks
   */
  def start(): Unit = {
    logger.info("Starting recommendation service...")
    
    try {
      // Initialize queues
      configureQueues()
      
      // Start consumers
      startRecommendationRequestConsumer()
      startProcessedEventsConsumer()
      startDataManagerSyncConsumer()
      
      // Schedule background tasks
      scheduleBackgroundTasks()
      
      // Preload data
      preloadData()
      
      // Register health checks
      registerHealthChecks()
      
      logger.info("Recommendation service started successfully")
    } catch {
      case e: Exception => 
        logger.error("Failed to start recommendation service", e)
        throw e
    }
  }
  
  /**
   * Configures RabbitMQ queues
   */
  private def configureQueues(): Unit = {
    try {
      // Queue declarations with updated names
      channelConsumer.queueDeclare(
        RabbitMQManager.Queues.PROCESSED_EVENTS, 
        true, false, false, null
      )
      
      channelPublisher.queueDeclare(
        RabbitMQManager.Queues.RECOMMENDATIONS, 
        true, false, false, null
      )
      
      // Removed obsolete queue declarations
    } catch {
      case e: Exception =>
        logger.error("Failed to configure queues", e)
        throw e
    }
  }
  
  /**
   * Starts the consumer for recommendation requests
   */
  private def startRecommendationRequestConsumer(): Unit = {
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        decode[RecommendationRequest](message) match {
          case Right(request) =>
            logger.debug(s"Received recommendation request for user ${request.userId}")
            
            try {
              // Check circuit breaker
              if (isCircuitOpen) {
                logger.warn(s"Circuit breaker open, rejecting recommendation request for user ${request.userId}")
                channelConsumer.basicNack(delivery.getEnvelope.getDeliveryTag, false, false)
                return
              }
              
              // Check recent recommendations cache to avoid duplicate processing
              recentRecommendations.get(request.userId).foreach { case (cachedRec, timestamp) =>
                if ((System.currentTimeMillis() - timestamp) < recommendationCacheTTL) {
                  // Use cached recommendation if recent enough
                  logger.debug(s"Using cached recommendation for user ${request.userId}")
                  publishRecommendation(cachedRec)
                  channelConsumer.basicAck(delivery.getEnvelope.getDeliveryTag, false)
                  return
                }
              }
              
              // Generate recommendations with timeout
              val recommendationsFuture = generateRecommendations(request)
                .map { recommendations =>
                  // Store in cache
                  recentRecommendations.put(
                    request.userId, 
                    (recommendations, System.currentTimeMillis())
                  )
                  
                  // Publish recommendations
                  publishRecommendation(recommendations)
                  recommendations
                }
              
              // Set a timeout for the future
              try {
                Await.result(recommendationsFuture, 5.seconds)
                channelConsumer.basicAck(delivery.getEnvelope.getDeliveryTag, false)
                resetCircuitBreaker()
              } catch {
                case e: TimeoutException =>
                  logger.error(s"Recommendation generation timed out for user ${request.userId}")
                  recordFailure()
                  channelConsumer.basicNack(delivery.getEnvelope.getDeliveryTag, false, true) // Requeue
                  
                case e: Exception =>
                  logger.error(s"Failed to generate recommendations for user ${request.userId}: ${e.getMessage}", e)
                  recordFailure()
                  channelConsumer.basicNack(delivery.getEnvelope.getDeliveryTag, false, true) // Requeue
              }
            } catch {
              case ex: Exception =>
                logger.error(s"Error processing recommendation request for user ${request.userId}: ${ex.getMessage}", ex)
                recordFailure()
                channelConsumer.basicNack(delivery.getEnvelope.getDeliveryTag, false, true)
            }
            
          case Left(error) =>
            logger.error(s"Failed to decode recommendation request: ${error.getMessage}")
            channelConsumer.basicReject(delivery.getEnvelope.getDeliveryTag, false) // Don't requeue invalid messages
        }
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Recommendation consumer cancelled: $consumerTag")
        
        // Try to reconnect
        try {
          Thread.sleep(5000)
          channelConsumer = RabbitMQManager.createChannel("recommender-channel")
          startRecommendationRequestConsumer()
        } catch {
          case e: Exception =>
            logger.error("Failed to reconnect recommendation consumer", e)
        }
      }
    }
    
    // Start consuming
    channelConsumer.basicQos(1) // Process one message at a time
    channelConsumer.basicConsume(
      RabbitMQManager.Queues.PROCESSED_EVENTS,
      false,  // manual ack
      deliverCallback,
      cancelCallback
    )
    
    logger.info("Recommendation request consumer started")
  }
  
  /**
   * Starts the consumer for processed events to maintain the in-memory model
   */
  private def startProcessedEventsConsumer(): Unit = {
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        decode[ProcessedEvent](message) match {
          case Right(event) =>
            try {
              // Update in-memory model
              updateUserEventCache(event)
              channelConsumer.basicAck(delivery.getEnvelope.getDeliveryTag, false)
            } catch {
              case e: Exception =>
                logger.error(s"Error processing event: ${e.getMessage}", e)
                channelConsumer.basicNack(delivery.getEnvelope.getDeliveryTag, false, true)
            }
            
          case Left(error) =>
            logger.error(s"Failed to decode processed event: ${error.getMessage}")
            channelConsumer.basicReject(delivery.getEnvelope.getDeliveryTag, false)
        }
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"Processed events consumer cancelled: $consumerTag")
        
        // Try to reconnect
        try {
          Thread.sleep(5000)
          channelConsumer = RabbitMQManager.createChannel("recommender-channel")
          startProcessedEventsConsumer()
        } catch {
          case e: Exception =>
            logger.error("Failed to reconnect processed events consumer", e)
        }
      }
    }
    
    // Start consuming
    channelConsumer.basicQos(10) // Process up to 10 messages at a time
    channelConsumer.basicConsume(
      RabbitMQManager.Queues.PROCESSED_EVENTS,
      false,  // manual ack
      deliverCallback,
      cancelCallback
    )
    
    logger.info("Processed events consumer started")
  }
  
  /**
   * Starts the consumer for DataManager sync messages
   */
  private def startDataManagerSyncConsumer(): Unit = {
    val deliverCallback = new DeliverCallback {
      override def handle(consumerTag: String, delivery: com.rabbitmq.client.Delivery): Unit = {
        val message = new String(delivery.getBody, StandardCharsets.UTF_8)
        
        try {
          decode[Map[String, String]](message) match {
            case Right(notification) =>
              notification.get("type") match {
                case Some("user") =>
                  notification.get("id").foreach { idStr =>
                    Try(idStr.toLong).foreach { userId =>
                      // Invalidate user cache
                      userProfileCache.get(userId).foreach(_ => userProfileCache.put(userId, null))
                      userFriendsCache.remove(userId)
                    }
                  }
                  
                case Some("event") =>
                  notification.get("id").foreach { idStr =>
                    Try(idStr.toLong).foreach { eventId =>
                      // Invalidate event cache
                      eventCache.remove(eventId)
                    }
                  }
                  
                case Some(other) =>
                  logger.debug(s"Received data update notification of type: $other")
                  
                case None =>
                  logger.warn("Received data update notification without type")
              }
              
            case Left(error) =>
              logger.error(s"Failed to decode data manager notification: ${error.getMessage}")
          }
          
          channelConsumer.basicAck(delivery.getEnvelope.getDeliveryTag, false)
        } catch {
          case e: Exception =>
            logger.error(s"Error processing data manager notification: ${e.getMessage}", e)
            channelConsumer.basicNack(delivery.getEnvelope.getDeliveryTag, false, true)
        }
      }
    }
    
    val cancelCallback = new CancelCallback {
      override def handle(consumerTag: String): Unit = {
        logger.warn(s"DataManager sync consumer cancelled: $consumerTag")
      }
    }
    
    // Start consuming
    channelConsumer.basicConsume(
      RabbitMQManager.Queues.HISTORICAL_DATA,
      false, // manual ack
      deliverCallback,
      cancelCallback
    )
    
    logger.info("DataManager sync consumer started")
  }
  
  /**
   * Schedule background tasks for cache maintenance and metrics reporting
   */
  private def scheduleBackgroundTasks(): Unit = {
    // Schedule LRU cache cleanup
    scheduler.scheduleAtFixedRate(() => {
      try {
        // Clean up old recommendations cache
        val now = System.currentTimeMillis()
        recentRecommendations.retain { (_, value) =>
          val (_, timestamp) = value
          (now - timestamp) < recommendationCacheTTL
        }
        
        // Regularly update similarity matrices
        updateSimilarityMatrices()
      } catch {
        case e: Exception =>
          logger.error("Error in cache maintenance task", e)
      }
    }, 5, 5, TimeUnit.MINUTES)
    
    // Report metrics periodically
    scheduler.scheduleAtFixedRate(() => {
      try {
        logMetrics()
      } catch {
        case e: Exception =>
          logger.error("Error in metrics reporting task", e)
      }
    }, 1, 1, TimeUnit.MINUTES)
  }
  
  /**
   * Preload initial data into in-memory caches for faster startup
   */
  private def preloadData(): Unit = {
    logger.info("Preloading data into in-memory caches...")
    
    try {
      // Load active events
      Await.result(dataManager.getAllActiveEvents().map { events =>
        events.foreach { event =>
          event.id.foreach { id =>
            eventCache.put(id, event)
          }
        }
        logger.info(s"Preloaded ${events.size} active events")
      }, 30.seconds)
    } catch {
      case e: Exception =>
        logger.error("Error preloading data", e)
    }
  }
  
  /**
   * Update the user events cache with a new processed event
   */
  private def updateUserEventCache(event: ProcessedEvent): Unit = {
    recentUserEvents.getOrElseUpdate(event.userId, mutable.Queue.empty[ProcessedEvent])
      .synchronized {
        val queue = recentUserEvents(event.userId)
        queue.enqueue(event)
        // Keep only the last 100 events per user
        while (queue.size > 100) {
          queue.dequeue()
        }
      }
  }
  
  /**
   * Update similarity matrices between items based on user behavior
   */
  private def updateSimilarityMatrices(): Unit = {
    // This would be a background job to compute item-item similarities
    // For now, just a placeholder
    logger.debug("Updating similarity matrices...")
    
    // In a real implementation, this would analyze co-occurrence patterns and update the similarityMatrixCache
  }
  
  /**
   * Generate recommendations for a user
   */
  def generateRecommendations(request: RecommendationRequest): Future[Recommendation] = {
    logger.debug(s"Generating recommendations for user ${request.userId}")
    
    val startTime = System.nanoTime()
    totalRequests.incrementAndGet()
    
    val resultPromise = Promise[Recommendation]()
    
    // Execute recommendation generation with timeout protection
    Future {
      try {
        val result = Await.result(generateRecommendationsInternal(request), 4.seconds)
        resultPromise.success(result)
      } catch {
        case e: Exception =>
          logger.error(s"Error generating recommendations: ${e.getMessage}", e)
          resultPromise.failure(e)
      }
    }
    
    // When complete, update metrics
    resultPromise.future.onComplete {
      case Success(recommendation) =>
        val endTime = System.nanoTime()
        val processingTimeMs = (endTime - startTime) / 1000000
        updateMetrics(processingTimeMs, success = true)
        successfulRequests.incrementAndGet()
        
      case Failure(e) =>
        val endTime = System.nanoTime()
        val processingTimeMs = (endTime - startTime) / 1000000
        updateMetrics(processingTimeMs, success = false)
        failedRequests.incrementAndGet()
    }
    
    resultPromise.future
  }
  
  /**
   * Internal implementation of recommendation generation
   */
  private def generateRecommendationsInternal(request: RecommendationRequest): Future[Recommendation] = {
    // Time the operation
    requestTimer.time {
      // Create parallel futures for all data we need
      val userEventsFuture = getUserEvents(request.userId)
      val userProfileFuture = getUserProfile(request.userId)
      val userFriendsFuture = getUserFriends(request.userId)
      val activeEventsFuture = getActiveEvents()
      
      for {
        userEvents <- userEventsFuture
        userProfile <- userProfileFuture
        userFriends <- userFriendsFuture
        activeEvents <- activeEventsFuture
        
        // Generate recommendation items from all available events
        recommendedItems = generateRecommendationItems(
          request.userId,
          userEvents,
          userProfile,
          userFriends,
          activeEvents,
          request.contextItemId
        )
        
        // Calculate total recommendation score
        totalScore = if (recommendedItems.nonEmpty) {
          recommendedItems.map(_.score).sum / recommendedItems.size
        } else 0.0
        
        // Create final recommendation
        recommendation = Recommendation(
          id = None,
          userId = request.userId,
          recommendedItems = recommendedItems,
          score = totalScore,
          timestamp = System.currentTimeMillis()
        )
        
        // Store in repository
        storedRec <- dataManager.storeRecommendation(recommendation)
      } yield {
        logger.info(s"Generated and stored recommendation for user ${request.userId} " +
                    s"with ${recommendedItems.size} items")
        
        // Update metrics
        recommendationsCounter.inc(recommendedItems.size)
        
        // Track scores in histogram
        recommendedItems.foreach(r => scoreHistogram.update(r.score.toLong))
        
        // Update cache metrics
        val hits = recentRecommendations.size
        val total = recentRecommendations.size + userProfileCache.size
        if (total > 0) {
          cacheHitRatio.set((hits * 100) / total)
        }
        
        storedRec
      }
    }
  }
  
  /**
   * Get user events from memory first, then from repository if needed
   */
  private def getUserEvents(userId: Long): Future[List[ProcessedEvent]] = {
    recentUserEvents.get(userId) match {
      case Some(events) if events.nonEmpty =>
        logger.debug(s"Using in-memory events for user $userId: ${events.size} events")
        Future.successful(events.toList)
        
      case _ =>
        // Fetch from repository
        logger.debug(s"Fetching events for user $userId from repository")
        dataManager.getProcessedEventsForUser(userId, 100).map { events =>
          // Populate the cache
          val queue = recentUserEvents.getOrElseUpdate(userId, mutable.Queue.empty[ProcessedEvent])
          queue.synchronized {
            queue.clear()
            events.foreach(queue.enqueue(_))
          }
          events
        }
    }
  }
  
  /**
   * Get user profile from memory cache first, then from repository if needed
   */
  private def getUserProfile(userId: Long): Future[Option[User]] = {
    userProfileCache.get(userId) match {
      case Some(user) if user != null =>
        logger.debug(s"Using cached user profile for user $userId")
        Future.successful(Some(user))
        
      case _ =>
        // Fetch from repository
        logger.debug(s"Fetching user profile for user $userId from repository")
        dataManager.getUserProfile(userId).map { userOpt =>
          userOpt.foreach(user => userProfileCache.put(userId, user))
          userOpt
        }
    }
  }
  
  /**
   * Get user friends from memory first, then from DataManager if needed
   */
  private def getUserFriends(userId: Long): Future[List[UserFriend]] = {
    userFriendsCache.get(userId) match {
      case Some(friends) =>
        logger.debug(s"Using cached friends data for user $userId: ${friends.size} friends")
        Future.successful(friends)
        
      case None =>
        // Fetch from DataManager instead of repository
        logger.debug(s"Fetching friends for user $userId from DataManager")
        dataManager.getUserFriends(userId).map { friends =>
          userFriendsCache.put(userId, friends)
          friends
        }
    }
  }
  
  /**
   * Get active events from memory first, then from repository if needed
   */
  private def getActiveEvents(): Future[List[Event]] = {
    if (eventCache.nonEmpty) {
      logger.debug(s"Using cached events: ${eventCache.size} events")
      Future.successful(eventCache.values.toList)
    } else {
      // Fetch from repository
      logger.debug("Fetching active events from repository")
      dataManager.getAllActiveEvents().map { events =>
        // Update cache
        events.foreach { event =>
          event.id.foreach(id => eventCache.put(id, event))
        }
        events
      }
    }
  }
  
  /**
   * Generate recommendation items based on all available data
   */
  private def generateRecommendationItems(
    userId: Long,
    userEvents: List[ProcessedEvent],
    userProfile: Option[User],
    userFriends: List[UserFriend],
    activeEvents: List[Event],
    contextItemId: Option[Long]
  ): List[RecommendedItem] = {
    // Filter out events the user has already interacted with
    val userInteractedItemIds = userEvents.flatMap(_.itemId).toSet
    val candidateEvents = activeEvents.filter(event => 
      event.id.exists(id => !userInteractedItemIds.contains(id))
    )
    
    if (candidateEvents.isEmpty) {
      logger.debug(s"No candidate events found for user $userId")
      return List.empty
    }
    
    logger.debug(s"Found ${candidateEvents.size} candidate events for user $userId")
    
    // Score each candidate event
    val scoredItems = candidateEvents.flatMap { event =>
      event.id.map { eventId =>
        // Specific item events
        val itemEvents = userEvents.filter(_.itemId.contains(eventId))
        
        // Calculate component scores
        val behaviorScore = calculateBehaviorScore(itemEvents)
        val contextualScore = calculateContextualScore(event, userProfile, userFriends)
        val historicalScore = calculateHistoricalScore(itemEvents, userEvents)
        
        // Apply weights to get final score
        val weightedScore = 
          (behaviorScore * CurrentBehaviorWeight) + 
          (contextualScore * ContextualWeight) + 
          (historicalScore * HistoricalWeight)
        
        // Generate reason
        val reason = generateRecommendationReason(behaviorScore, contextualScore, historicalScore)
        
        // Boost if this is the context item
        val finalScore = contextItemId match {
          case Some(id) if id == eventId => weightedScore * 1.2 // 20% boost for context item
          case _ => weightedScore
        }
        
        RecommendedItem(
          itemId = eventId,
          score = finalScore,
          reason = reason
        )
      }
    }
    
    // Sort by score and take top 10
    scoredItems.sortBy(-_.score).take(10)
  }
  
  /**
   * Calculates the behavior score based on frequency and recency (60% of total)
   */
  private def calculateBehaviorScore(itemEvents: List[ProcessedEvent]): Double = {
    if (itemEvents.isEmpty) return 0.0
    
    val now = System.currentTimeMillis()
    
    // Frequency component
    val frequencyScore = math.min(10.0 * itemEvents.size, 50.0)
    
    // Enhanced recency component - more granular time-based decay
    val recencyScore = if (itemEvents.exists(_.timestamp > now - DayInMillis)) {
      // Within last 24 hours - highest score
      50.0
    } else if (itemEvents.exists(_.timestamp > now - 3 * DayInMillis)) {
      // Within last 3 days
      30.0
    } else if (itemEvents.exists(_.timestamp > now - WeekInMillis)) {
      // Within last week
      15.0
    } else {
      // Older
      5.0
    }
    
    // Additional score for interaction types (purchases/registrations weighted higher than views)
    val interactionTypeScore = itemEvents.map { event =>
      event.eventType match {
        case "purchase" | "register" => 20.0
        case "click" => 5.0
        case _ => 1.0  // default for views and others
      }
    }.sum
    
    // Normalize to a reasonable range
    val totalScore = frequencyScore + recencyScore + interactionTypeScore
    math.min(100.0, totalScore)  // Cap at 100 to normalize
  }
  
  /**
   * Calculates contextual score based on geographic proximity and social factors (30% of total)
   */
  private def calculateContextualScore(
    event: Event, 
    userProfile: Option[User], 
    userFriends: List[UserFriend]
  ): Double = {
    var score = 0.0
    
    // 1. Geographic proximity scoring (up to 50% of contextual score)
    if (isSameLocation(userProfile, event)) {
      score += 50.0
    } else if (isNearby(userProfile.flatMap(_.location), event.location)) {
      score += 30.0
    } else if (isSameRegion(userProfile.flatMap(_.location), event.location)) {
      score += 15.0
    }
    
    // 2. Social factors scoring (up to 50% of contextual score)
    val eventParticipants = event.participants
    val friendIds = userFriends.map(_.friendId).toSet
    
    // Calculate how many friends are participating
    val participatingFriends = eventParticipants.intersect(friendIds)
    if (participatingFriends.nonEmpty) {
      // Add base score for having friends + additional per friend
      score += 30.0 + (participatingFriends.size * 5.0)
      
      // Add weighted score based on friendship strength
      val friendshipStrength = userFriends
        .filter(f => participatingFriends.contains(f.friendId))
        .map(_.strength)
        .sum(implicitly[Numeric[Double]])
      
      score += friendshipStrength * 10.0
    }
    
    math.min(100.0, score)  // Cap at 100
  }
  
  /**
   * Determines if a user and event are in the same location
   */
  private def isSameLocation(userProfile: Option[User], event: Event): Boolean = {
    // Extract locations
    val userLocation = userProfile.flatMap(_.location)
    val eventLocation = event.location
    
    // Simplified version for demonstration:
    (userLocation, eventLocation) match {
      case (Some(user), Some(event)) if user.contains(",") && event.contains(",") => 
        user.split(",")(0).trim.equalsIgnoreCase(event.split(",")(0).trim)  // Compare city/first part
      case _ => false
    }
  }
  
  /**
   * Calculates historical preference score based on long-term patterns (10% of total)
   */
  private def calculateHistoricalScore(itemEvents: List[ProcessedEvent], allUserEvents: List[ProcessedEvent]): Double = {
    if (allUserEvents.isEmpty) return 0.0
    
    // Extract categories/topics from current item
    val itemCategories = itemEvents.flatMap(event => 
      event.properties.get("category")  // properties is already a Map[String, String]
    ).toSet
    
    if (itemCategories.isEmpty) return 0.0
    
    // Find historical events with matching categories
    val categoryMatchCount = allUserEvents
      .flatMap(event => 
        event.properties.get("category")  // properties is already a Map[String, String]
      )
      .count(category => itemCategories.contains(category))
    
    // Calculate score based on category matches
    val score = categoryMatchCount * 15.0
    
    math.min(100.0, score)  // Cap at 100 to normalize
  }
  
  /**
   * Helper method to check if two locations are nearby
   */
  private def isNearby(userLocation: Option[String], eventLocation: Option[String]): Boolean = {
    // In a real implementation, this would use geocoding and distance calculations
    // Simplified version for demonstration:
    (userLocation, eventLocation) match {
      case (Some(user), Some(event)) => 
        user.split(",")(0).trim.equalsIgnoreCase(event.split(",")(0).trim)  // Compare city/first part
      case _ => false
    }
  }
  
  /**
   * Helper method to check if two locations are in the same region
   */
  private def isSameRegion(userLocation: Option[String], eventLocation: Option[String]): Boolean = {
    // In a real implementation, this would use region/state/province comparison
    // Simplified version for demonstration:
    (userLocation, eventLocation) match {
      case (Some(user), Some(event)) if user.contains(",") && event.contains(",") => 
        user.split(",")(1).trim.equalsIgnoreCase(event.split(",")(1).trim)  // Compare region/second part
      case _ => false
    }
  }
  
  /**
   * Generates a human-readable recommendation reason based on score components
   */
  private def generateRecommendationReason(
    behaviorScore: Double, 
    contextualScore: Double,
    historicalScore: Double
  ): String = {
    val factors = Seq(
      (behaviorScore * CurrentBehaviorWeight, "your recent activity"),
      (contextualScore * ContextualWeight, "location and social connections"),
      (historicalScore * HistoricalWeight, "your interests")
    ).sortBy(-_._1)
    
    // Primary factor has highest weighted score
    val primaryFactor = factors.head._2
    
    // Add secondary factor if it's significant (at least 40% of the primary factor)
    val reasonText = if (factors.size > 1 && factors(1)._1 >= factors.head._1 * 0.4) {
      s"Based on $primaryFactor and ${factors(1)._2}"
    } else {
      s"Based on $primaryFactor"
    }
    
    reasonText
  }
  
  /**
   * Publishes a recommendation to the recommendations queue.
   */
  private def publishRecommendation(recommendation: Recommendation): Unit = {
    try {
      val message = recommendation.asJson.noSpaces
      
      val props = new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .deliveryMode(2) // persistent
        .timestamp(new java.util.Date())
        .build()
      
      channelPublisher.basicPublish(
        RabbitMQManager.Exchanges.USER_ACTIONS,
        "recommendation.result." + recommendation.userId, // Routing key appropriée
        props,
        message.getBytes(StandardCharsets.UTF_8)
      )
      logger.info(s"Published recommendations for user ${recommendation.userId}")
    } catch {
      case e: Exception => 
        logger.error(s"Error publishing recommendation: ${e.getMessage}", e)
    }
  }
  
  /**
   * Update metrics with a new response time measurement
   */
  private def updateMetrics(responseTimeMs: Long, success: Boolean): Unit = {
    metricsLock.synchronized {
      // Update total response time
      totalResponseTime.addAndGet(responseTimeMs)
      
      // Update min/max
      var current = minResponseTime.get()
      while (responseTimeMs < current) {
        minResponseTime.compareAndSet(current, responseTimeMs)
        current = minResponseTime.get()
      }
      
      current = maxResponseTime.get()
      while (responseTimeMs > current) {
        maxResponseTime.compareAndSet(current, responseTimeMs)
        current = maxResponseTime.get()
      }
      
      // Update sliding window
      recentResponseTimes.enqueue(responseTimeMs)
      if (recentResponseTimes.size > maxSlidingWindowSize) {
        recentResponseTimes.dequeue()
      }
      
      // Update average
      val count = totalRequests.get()
      if (count > 0) {
        avgResponseTime.set(totalResponseTime.get() / count)
      }
    }
  }
  
  /**
   * Record a failure for circuit breaker
   */
  private def recordFailure(): Unit = {
    consecutiveFailures += 1
    lastFailureTime = System.currentTimeMillis()
    
    if (consecutiveFailures >= failureThreshold) {
      logger.warn(s"Circuit breaker tripped after $consecutiveFailures consecutive failures")
      circuitOpen.set(true)
    }
  }
  
  /**
   * Reset circuit breaker on success
   */
  private def resetCircuitBreaker(): Unit = {
    consecutiveFailures = 0
    if (circuitOpen.get() && System.currentTimeMillis() - lastFailureTime > resetTimeout) {
      logger.info("Circuit breaker reset")
      circuitOpen.set(false)
    }
  }
  
  /**
   * Check if circuit breaker is open
   */
  private def isCircuitOpen: Boolean = {
    // If circuit is open but the reset timeout has elapsed, try again
    if (circuitOpen.get() && System.currentTimeMillis() - lastFailureTime > resetTimeout) {
      logger.info("Circuit breaker reset after timeout")
      circuitOpen.set(false)
      false
    } else {
      circuitOpen.get()
    }
  }
  
  /**
   * Log current metrics
   */
  private def logMetrics(): Unit = {
    val total = totalRequests.get()
    val successful = successfulRequests.get()
    val failed = failedRequests.get()
    val avgTime = avgResponseTime.get()
    val minTime = minResponseTime.get()
    val maxTime = maxResponseTime.get()
    
    // Calculate recent average from sliding window
    val recentAvg = if (recentResponseTimes.nonEmpty) {
      recentResponseTimes.sum / recentResponseTimes.size
    } else 0
    
    logger.info(
      s"""Recommender Performance Metrics:
         |  Total Requests: $total (Successful: $successful, Failed: $failed)
         |  Response Time (ms): Avg=$avgTime, Min=${if (minTime == Long.MaxValue) 0 else minTime}, Max=$maxTime
         |  Recent Average Response Time (last ${recentResponseTimes.size} requests): ${recentAvg}ms
         |  Circuit Breaker: ${if (circuitOpen.get()) "OPEN" else "CLOSED"}
         |  Cache Stats: User Profiles=${userProfileCache.size}, Events=${eventCache.size}, User Events=${recentUserEvents.size}
         |""".stripMargin)
  }
  
  /**
   * Clean shutdown of resources
   */
  def close(): Unit = {
    try {
      // Close channels
      RabbitMQManager.closeChannel("recommender-channel")
      RabbitMQManager.closeChannel("recommender-publisher")
      
      // Shutdown scheduler
      scheduler.shutdown()
      try {
        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
          scheduler.shutdownNow()
        }
      } catch {
        case _: InterruptedException => 
          scheduler.shutdownNow()
      }
      
      logger.info("Recommender closed")
    } catch {
      case e: Exception =>
        logger.error("Error closing recommender", e)
    }
  }
  
  /**
   * Get current performance metrics as a formatted string
   */
  def getPerformanceMetrics: String = {
    val total = totalRequests.get()
    val avg = avgResponseTime.get()
    val min = if (minResponseTime.get() == Long.MaxValue) 0 else minResponseTime.get()
    val max = maxResponseTime.get()
    val successRate = if (total > 0) (successfulRequests.get() * 100.0) / total else 0.0
    
    f"""Recommender Performance:
       |  Total Requests: $total
       |  Success Rate: $successRate%.2f%%
       |  Response Time: Avg=${avg}ms, Min=${min}ms, Max=${max}ms
       |""".stripMargin
  }

  /**
   * Perform a healthcheck on the recommender component
   * @return true if the component is healthy, false otherwise
   */
  def healthCheck(): Boolean = {
    try {
      // Vérifier si les caches sont accessibles
      val userCacheOk = userProfileCache != null
      val eventCacheOk = eventCache != null
      val recentEventsOk = recentUserEvents != null
      
      // Vérifier si RabbitMQ est accessible
      val rmqOk = channelConsumer != null && channelConsumer.isOpen
      
      // Utiliser la méthode healthCheck de DataManager
      val dataManagerOk = Try(dataManager.healthCheck()).getOrElse(false)
      
      // Enregistrer les résultats dans le registry pour le monitoring
      HealthCheckRegistry.register(
        HealthCheckRegistry.createHealthCheck("recommender", "components") {
          val status = if (userCacheOk && eventCacheOk && recentEventsOk && rmqOk && dataManagerOk) {
            HealthStatus.Healthy
          } else {
            HealthStatus.Degraded
          }
          
          HealthCheckResult(
            component = "recommender",
            check = "components",
            status = status,
            message = s"Recommender components health: UserCache=$userCacheOk, EventCache=$eventCacheOk, RecentEvents=$recentEventsOk, RMQ=$rmqOk, DataManager=$dataManagerOk",
            details = Map(
              "userCache" -> userCacheOk.toString,
              "eventCache" -> eventCacheOk.toString,
              "recentEvents" -> recentEventsOk.toString,
              "rabbitmq" -> rmqOk.toString,
              "dataManager" -> dataManagerOk.toString
            )
          )
        }
      )
      
      userCacheOk && eventCacheOk && recentEventsOk && rmqOk && dataManagerOk
    } catch {
      case e: Exception =>
        false
    }
  }

  /**
   * Améliore la performance en préchargant les données pour un utilisateur
   * avant de générer des recommandations
   */
  def preloadUserData(userId: Long): Future[Boolean] = {
    val futures = List(
      getUserProfile(userId).map(_ => true),
      getUserEvents(userId).map(_ => true),
      getUserFriends(userId).map(_ => true)
    )
    
    Future.sequence(futures).map(_ => true).recover { 
      case e: Exception => 
        logger.warn(s"Failed to preload data for user $userId: ${e.getMessage}")
        false
    }
  }
}