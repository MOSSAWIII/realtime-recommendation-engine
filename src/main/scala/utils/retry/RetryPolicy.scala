package utils.retry

import scala.concurrent.{Future, ExecutionContext, Promise, duration}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure, Random}
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean, AtomicReference}
import java.util.concurrent.{ScheduledExecutorService, Executors, TimeUnit}
import ErrorCategory._
import org.slf4j.LoggerFactory

/**
 * Retry policy configuration
 */
case class RetryConfig(
  maxRetries: Int = 3,
  initialBackoffMs: Long = 100,
  maxBackoffMs: Long = 10000,
  backoffFactor: Double = 2.0,
  jitterFactor: Double = 0.2,
  timeout: FiniteDuration = 30.seconds,
  retriableCategories: Set[ErrorCategory] = Set(ConnectionError, ProcessingError, ExternalServiceError)
)

/**
 * Circuit breaker state
 */
object CircuitBreakerState extends Enumeration {
  type CircuitBreakerState = Value
  val Closed, HalfOpen, Open = Value
}

/**
 * Circuit breaker configuration
 */
case class CircuitBreakerConfig(
  failureThreshold: Int = 5,
  resetTimeout: FiniteDuration = 30.seconds,
  halfOpenAttempts: Int = 1
)

/**
 * Retry outcome tracking
 */
case class RetryOutcome[T](
  result: Either[Throwable, T],
  attempts: Int,
  totalDuration: FiniteDuration,
  success: Boolean
)

/**
 * Interface for retry operations
 */
trait RetryPolicy {
  def retrySync[T](operation: => T): T
  def retryAsync[T](operation: => Future[T])(implicit ec: ExecutionContext): Future[T]
}

/**
 * Implementation of retry policy with exponential backoff
 */
class ExponentialBackoffRetry(
  component: String,
  config: RetryConfig = RetryConfig()
) extends RetryPolicy {
  private val logger = LoggerFactory.getLogger(s"retry.$component")
  private val errorHandler = ErrorHandler.forComponent(component)
  
  // Executor for scheduling delayed retries
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(2)
  
  /**
   * Calculate next backoff interval with jitter
   */
  private def calculateBackoff(attempt: Int): Long = {
    val base = Math.min(
      config.maxBackoffMs,
      config.initialBackoffMs * Math.pow(config.backoffFactor, attempt.toDouble)
    )
    
    // Add jitter to avoid thundering herd problem
    val jitter = (Random.nextDouble() * 2.0 - 1.0) * config.jitterFactor * base
    Math.max(0, Math.round(base + jitter))
  }
  
  /**
   * Check if an error is retriable based on its category
   */
  private def isRetriable(error: AppError): Boolean = {
    config.retriableCategories.contains(error.category) || error.retriable
  }
  
  /**
   * Retry a synchronous operation with backoff
   */
  override def retrySync[T](operation: => T): T = {
    var attempts = 0
    var lastError: Option[Throwable] = None
    val startTime = System.nanoTime()
    
    while (attempts <= config.maxRetries) {
      try {
        val result = operation
        val duration = Duration.fromNanos(System.nanoTime() - startTime)
        logger.debug(s"Operation succeeded after ${attempts} attempts in ${duration.toMillis}ms")
        return result
      } catch {
        case ex: Throwable =>
          attempts += 1
          val error = errorHandler.createError("retrySync", ex)
          
          if (attempts <= config.maxRetries && isRetriable(error)) {
            val backoffMs = calculateBackoff(attempts)
            logger.warn(s"Attempt $attempts failed, retrying in ${backoffMs}ms: ${ex.getMessage}")
            Thread.sleep(backoffMs)
            lastError = Some(ex)
          } else {
            logger.error(s"Operation failed after $attempts attempts, not retrying", ex)
            throw ex
          }
      }
    }
    
    // If we get here, all retries failed
    throw lastError.getOrElse(
      new RuntimeException(s"Operation failed after ${config.maxRetries} attempts with unknown error")
    )
  }
  
  /**
   * Retry an asynchronous operation with backoff
   */
  override def retryAsync[T](operation: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    val attempts = new AtomicInteger(0)
    val startTime = System.nanoTime()
    
    def attemptOperation(): Unit = {
      try {
        val future = operation
        
        future.onComplete {
          case Success(result) =>
            val duration = Duration.fromNanos(System.nanoTime() - startTime)
            logger.debug(s"Async operation succeeded after ${attempts.get()} attempts in ${duration.toMillis}ms")
            promise.success(result)
            
          case Failure(ex) =>
            val currentAttempt = attempts.incrementAndGet()
            val error = errorHandler.createError("retryAsync", ex)
            
            if (currentAttempt <= config.maxRetries && isRetriable(error)) {
              val backoffMs = calculateBackoff(currentAttempt)
              logger.warn(s"Async attempt $currentAttempt failed, retrying in ${backoffMs}ms: ${ex.getMessage}")
              
              // Schedule retry after backoff delay
              scheduler.schedule(
                new Runnable { def run(): Unit = attemptOperation() },
                backoffMs,
                TimeUnit.MILLISECONDS
              )
            } else {
              val duration = Duration.fromNanos(System.nanoTime() - startTime)
              logger.error(s"Async operation failed after $currentAttempt attempts in ${duration.toMillis}ms, not retrying", ex)
              promise.failure(ex)
            }
        }
      } catch {
        case ex: Throwable =>
          val currentAttempt = attempts.incrementAndGet()
          val error = errorHandler.createError("retryAsync", ex)
          
          if (currentAttempt <= config.maxRetries && isRetriable(error)) {
            val backoffMs = calculateBackoff(currentAttempt)
            logger.warn(s"Async attempt $currentAttempt failed with exception, retrying in ${backoffMs}ms: ${ex.getMessage}")
            
            // Schedule retry after backoff delay
            scheduler.schedule(
              new Runnable { def run(): Unit = attemptOperation() },
              backoffMs,
              TimeUnit.MILLISECONDS
            )
          } else {
            logger.error(s"Async operation exception after $currentAttempt attempts, not retrying", ex)
            promise.failure(ex)
          }
      }
    }
    
    // Start first attempt
    attemptOperation()
    
    // Return a future with timeout
    val timeoutFuture = promise.future.withTimeout(config.timeout)
      .recoverWith { 
        case ex: java.util.concurrent.TimeoutException =>
          val timeoutError = errorHandler.createError(
            "retryAsync", 
            s"Operation timed out after ${config.timeout.toMillis}ms",
            ConnectionError
          )
          errorHandler.handleError(timeoutError)
          Future.failed(ex)
      }
    
    timeoutFuture
  }
}

/**
 * Circuit breaker implementation to prevent cascading failures
 */
class CircuitBreaker(
  component: String,
  operation: String,
  config: CircuitBreakerConfig = CircuitBreakerConfig()
) {
  import CircuitBreakerState._
  
  private val logger = LoggerFactory.getLogger(s"circuitbreaker.$component.$operation")
  private val errorHandler = ErrorHandler.forComponent(component)
  
  private val state = new AtomicReference[CircuitBreakerState](Closed)
  private val failureCount = new AtomicInteger(0)
  private val lastStateChange = new AtomicReference[Long](System.currentTimeMillis())
  private val scheduler = Executors.newSingleThreadScheduledExecutor()
  
  /**
   * Execute an operation through the circuit breaker
   */
  def execute[T](block: => T): T = {
    checkState()
    
    val currentState = state.get()
    
    if (currentState == Open) {
      val error = errorHandler.createError(
        operation, 
        s"Circuit breaker is open for $operation", 
        SystemError,
        Map("state" -> "open", "lastChange" -> lastStateChange.get().toString)
      )
      errorHandler.handleError(error)
      throw new RuntimeException(s"Circuit breaker is open for $operation")
    }
    
    try {
      val result = block
      recordSuccess()
      result
    } catch {
      case ex: Throwable =>
        recordFailure(ex)
        throw ex
    }
  }
  
  /**
   * Execute an asynchronous operation through the circuit breaker
   */
  def executeAsync[T](block: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    checkState()
    
    val currentState = state.get()
    
    if (currentState == Open) {
      val error = errorHandler.createError(
        operation, 
        s"Circuit breaker is open for $operation", 
        SystemError,
        Map("state" -> "open", "lastChange" -> lastStateChange.get().toString)
      )
      errorHandler.handleError(error)
      return Future.failed(new RuntimeException(s"Circuit breaker is open for $operation"))
    }
    
    try {
      val future = block
      
      future.onComplete {
        case Success(_) => recordSuccess()
        case Failure(ex) => recordFailure(ex)
      }
      
      future
    } catch {
      case ex: Throwable =>
        recordFailure(ex)
        Future.failed(ex)
    }
  }
  
  private def recordSuccess(): Unit = {
    val currentState = state.get()
    
    if (currentState == HalfOpen) {
      // On success in half-open state, return to closed state
      if (state.compareAndSet(HalfOpen, Closed)) {
        lastStateChange.set(System.currentTimeMillis())
        failureCount.set(0)
        logger.info(s"Circuit breaker for $operation closed after successful attempt")
      }
    } else {
      // In closed state, just reset failure count
      failureCount.set(0)
    }
  }
  
  private def recordFailure(ex: Throwable): Unit = {
    val currentState = state.get()
    
    if (currentState == HalfOpen) {
      // On failure in half-open state, return to open state
      if (state.compareAndSet(HalfOpen, Open)) {
        lastStateChange.set(System.currentTimeMillis())
        logger.warn(s"Circuit breaker for $operation reopened after failed attempt: ${ex.getMessage}")
        
        // Schedule next retry
        scheduleReset()
      }
    } else if (currentState == Closed) {
      // In closed state, increment failure count and check threshold
      val count = failureCount.incrementAndGet()
      
      if (count >= config.failureThreshold) {
        if (state.compareAndSet(Closed, Open)) {
          lastStateChange.set(System.currentTimeMillis())
          logger.warn(s"Circuit breaker for $operation opened after $count consecutive failures")
          
          // Schedule a reset after the timeout
          scheduleReset()
        }
      }
    }
  }
  
  private def checkState(): Unit = {
    // If in open state, check if reset timeout has elapsed
    if (state.get() == Open) {
      val elapsedMs = System.currentTimeMillis() - lastStateChange.get()
      
      if (elapsedMs >= config.resetTimeout.toMillis) {
        attemptReset()
      }
    }
  }
  
  private def scheduleReset(): Unit = {
    scheduler.schedule(new Runnable {
      override def run(): Unit = attemptReset()
    }, config.resetTimeout.toMillis, TimeUnit.MILLISECONDS)
  }
  
  private def attemptReset(): Unit = {
    // Transition to half-open state to test if the issue is resolved
    if (state.compareAndSet(Open, HalfOpen)) {
      lastStateChange.set(System.currentTimeMillis())
      logger.info(s"Circuit breaker for $operation transitioned to half-open state")
    }
  }
  
  /**
   * Get the current state of the circuit breaker
   */
  def getState: CircuitBreakerState = state.get()
  
  /**
   * Get the time (in ms) since the last state change
   */
  def getTimeSinceStateChange: Long = {
    System.currentTimeMillis() - lastStateChange.get()
  }
  
  /**
   * Get current failure count
   */
  def getFailureCount: Int = failureCount.get()
  
  /**
   * Reset the circuit breaker to closed state (e.g., for testing)
   */
  def reset(): Unit = {
    state.set(Closed)
    failureCount.set(0)
    lastStateChange.set(System.currentTimeMillis())
    logger.info(s"Circuit breaker for $operation manually reset to closed state")
  }
  
  /**
   * Force the circuit breaker to open (e.g., for maintenance)
   */
  def forceOpen(): Unit = {
    state.set(Open)
    lastStateChange.set(System.currentTimeMillis())
    logger.info(s"Circuit breaker for $operation manually forced to open state")
  }
  
  /**
   * Shutdown the circuit breaker's scheduled executor
   */
  def shutdown(): Unit = {
    scheduler.shutdown()
  }
}

/**
 * Factory methods for creating retry policies
 */
object RetryPolicy {
  /**
   * Create a standard retry policy with default settings
   */
  def standard(component: String): RetryPolicy = {
    new ExponentialBackoffRetry(component)
  }
  
  /**
   * Create a retry policy with custom configuration
   */
  def create(component: String, config: RetryConfig): RetryPolicy = {
    new ExponentialBackoffRetry(component, config)
  }
  
  /**
   * Create an aggressive retry policy with more retries and longer timeout
   */
  def aggressive(component: String): RetryPolicy = {
    new ExponentialBackoffRetry(component, RetryConfig(
      maxRetries = 10,
      initialBackoffMs = 200,
      maxBackoffMs = 30000,
      backoffFactor = 2.0,
      timeout = 2.minutes
    ))
  }
  
  /**
   * Create a cautious retry policy with minimal retries
   */
  def cautious(component: String): RetryPolicy = {
    new ExponentialBackoffRetry(component, RetryConfig(
      maxRetries = 1,
      initialBackoffMs = 50,
      maxBackoffMs = 1000,
      backoffFactor = 1.5,
      timeout = 10.seconds
    ))
  }
}

/**
 * Extension methods for Future to work with retries and timeouts
 */
object FutureExtensions {
  /**
   * Add timeout to a Future
   */
  implicit class FutureWithTimeout[T](future: Future[T]) {
    def withTimeout(timeout: FiniteDuration)(implicit ec: ExecutionContext): Future[T] = {
      val promise = Promise[T]()
      
      val timer = Executors.newSingleThreadScheduledExecutor()
      val timeoutTask = new Runnable {
        override def run(): Unit = {
          promise.tryFailure(
            new java.util.concurrent.TimeoutException(s"Operation timed out after ${timeout.toMillis}ms")
          )
        }
      }
      
      val scheduledTask = timer.schedule(timeoutTask, timeout.length, timeout.unit)
      
      future.onComplete { result =>
        scheduledTask.cancel(false)
        timer.shutdown()
        promise.tryComplete(result)
      }
      
      promise.future
    }
  }
}

/**
 * Error configuration section for ConfigurationService
 */
class ErrorConfigSection(config: com.typesafe.config.Config) {
  val enableEnhancedLogging: Boolean = config.getBoolean("error.enhancedLogging")
  val reportingEnabled: Boolean = config.getBoolean("error.reportingEnabled")
  val stackTraceEnabled: Boolean = config.getBoolean("error.stackTraceEnabled")
  val maxRecentErrors: Int = config.getInt("error.maxRecentErrors")
}