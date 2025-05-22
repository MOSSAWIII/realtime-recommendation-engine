package utils.error

import org.slf4j.{Logger, LoggerFactory, MDC}
import scala.util.{Try, Success, Failure}
import scala.concurrent.{Future, ExecutionContext}
import scala.collection.concurrent.TrieMap
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import infrastructure.config.ConfigurationService

/**
 * Error classification system for standardizing errors across the application
 */
object ErrorCategory {
  // Main error categories
  sealed abstract class ErrorCategory(val code: String, val retriable: Boolean, val severity: Int)
  
  // Connection related errors (network, service connectivity)
  case object ConnectionError extends ErrorCategory("CONN", true, 2)
  
  // Data related errors (parsing, validation, schema)
  case object DataError extends ErrorCategory("DATA", false, 2)
  
  // Processing errors (business logic, computation)
  case object ProcessingError extends ErrorCategory("PROC", true, 3)
  
  // Validation errors (input validation, constraints)
  case object ValidationError extends ErrorCategory("VALD", false, 1)
  
  // System errors (resource exhaustion, configuration)
  case object SystemError extends ErrorCategory("SYS", false, 4)
  
  // Security errors (authentication, authorization)
  case object SecurityError extends ErrorCategory("SEC", false, 5)
  
  // External service errors
  case object ExternalServiceError extends ErrorCategory("EXT", true, 3)
  
  // Unknown or unclassified errors
  case object UnknownError extends ErrorCategory("UNK", false, 3)
  
  // Map for looking up categories by code
  val codeMap: Map[String, ErrorCategory] = Map(
    "CONN" -> ConnectionError,
    "DATA" -> DataError,
    "PROC" -> ProcessingError,
    "VALD" -> ValidationError,
    "SYS" -> SystemError,
    "SEC" -> SecurityError,
    "EXT" -> ExternalServiceError,
    "UNK" -> UnknownError
  )
  
  // Helper function to categorize exceptions
  def categorizeException(ex: Throwable): ErrorCategory = ex match {
    case _: java.net.SocketException => ConnectionError
    case _: java.net.ConnectException => ConnectionError
    case _: java.net.SocketTimeoutException => ConnectionError
    case _: com.rabbitmq.client.ShutdownSignalException => ConnectionError
    case _: java.sql.SQLException => DataError
    case _: io.circe.Error => DataError
    case _: java.lang.IllegalArgumentException => ValidationError
    case _: java.lang.IllegalStateException => SystemError
    case _: java.lang.OutOfMemoryError => SystemError
    case _: java.util.concurrent.TimeoutException => ConnectionError
    case _ => UnknownError
  }
}

/**
 * Standard application error model with context information
 */
case class AppError(
  id: String,
  message: String,
  category: ErrorCategory.ErrorCategory,
  component: String,
  operation: String,
  context: Map[String, String],
  cause: Option[Throwable],
  timestamp: Long = System.currentTimeMillis(),
  retriable: Boolean = false
) {
  def withContext(additionalContext: (String, String)*): AppError =
    this.copy(context = this.context ++ additionalContext)
    
  def withRetriable(isRetriable: Boolean): AppError =
    this.copy(retriable = isRetriable)
    
  def toPrettyString: String = {
    val contextString = if (context.nonEmpty) {
      context.map { case (k, v) => s"$k: $v" }.mkString(", ")
    } else "none"
    
    s"""
    |Error[$id]
    |Category: ${category.code}
    |Component: $component
    |Operation: $operation
    |Message: $message
    |Context: $contextString
    |Timestamp: ${Instant.ofEpochMilli(timestamp)}
    |Retriable: $retriable
    """.stripMargin
  }
}

/**
 * Centralized error handling utility with error classification and reporting
 */
class ErrorHandler(componentName: String) {
  private val logger = LoggerFactory.getLogger(s"errorhandler.$componentName")
  
  private val errorCounters = TrieMap[String, AtomicInteger]()
  private val errorConfig = ConfigurationService.getInstance.getConfigSection(new ErrorConfigSection(_))
  private val recentErrors = new java.util.concurrent.ConcurrentLinkedQueue[AppError]()
  
  // Maximum number of recent errors to keep
  private val MAX_RECENT_ERRORS = 100
  
  /**
   * Create standardized application error from an exception
   */
  def createError(
    operation: String,
    exception: Throwable,
    context: Map[String, String] = Map.empty
  ): AppError = {
    val errorId = UUID.randomUUID().toString.take(8)
    val category = ErrorCategory.categorizeException(exception)
    
    val appError = AppError(
      id = errorId,
      message = exception.getMessage,
      category = category,
      component = componentName,
      operation = operation,
      context = context,
      cause = Some(exception),
      retriable = category.retriable
    )
    
    // Track error for metrics and reporting
    trackError(appError)
    
    appError
  }
  
  /**
   * Create standardized application error from a message
   */
  def createError(
    operation: String,
    message: String,
    category: ErrorCategory.ErrorCategory,
    context: Map[String, String] = Map.empty
  ): AppError = {
    val errorId = UUID.randomUUID().toString.take(8)
    
    val appError = AppError(
      id = errorId,
      message = message,
      category = category,
      component = componentName,
      operation = operation,
      context = context,
      cause = None,
      retriable = category.retriable
    )
    
    // Track error for metrics and reporting
    trackError(appError)
    
    appError
  }
  
  /**
   * Handle an error with standardized logging and tracking
   */
  def handleError(error: AppError): Unit = {
    // Add MDC context for structured logging
    try {
      MDC.put("errorId", error.id)
      MDC.put("component", error.component)
      MDC.put("operation", error.operation)
      MDC.put("category", error.category.code)
      
      error.context.foreach { case (key, value) =>
        MDC.put(key, value)
      }
      
      // Log with appropriate severity based on error category
      error.category.severity match {
        case 1 => logger.info(s"${error.message} [${error.id}]")
        case 2 => logger.warn(s"${error.message} [${error.id}]", error.cause.orNull)
        case 3 | 4 => logger.error(s"${error.message} [${error.id}]", error.cause.orNull)
        case _ => logger.error(s"CRITICAL: ${error.message} [${error.id}]", error.cause.orNull)
      }
      
    } finally {
      // Clear MDC to prevent leaking context
      MDC.clear()
    }
  }
  
  /**
   * Execute an operation with standardized error handling
   */
  def withErrorHandling[T](operation: String, context: Map[String, String] = Map.empty)
                          (block: => T): Try[T] = {
    try {
      Success(block)
    } catch {
      case ex: Throwable =>
        val error = createError(operation, ex, context)
        handleError(error)
        Failure(ex)
    }
  }
  
  /**
   * Execute an asynchronous operation with standardized error handling
   */
  def withAsyncErrorHandling[T](operation: String, context: Map[String, String] = Map.empty)
                              (block: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    try {
      val future = block
      
      future.recoverWith {
        case ex: Throwable =>
          val error = createError(operation, ex, context)
          handleError(error)
          Future.failed(ex)
      }
    } catch {
      case ex: Throwable =>
        val error = createError(operation, ex, context)
        handleError(error)
        Future.failed(ex)
    }
  }
  
  /**
   * Track error for metrics and reporting
   */
  private def trackError(error: AppError): Unit = {
    // Update error counters
    val key = s"${error.component}.${error.category.code}.${error.operation}"
    val counter = errorCounters.getOrElseUpdate(key, new AtomicInteger(0))
    counter.incrementAndGet()
    
    // Add to recent errors queue
    recentErrors.add(error)
    
    // Trim the queue if it gets too large
    while (recentErrors.size() > MAX_RECENT_ERRORS) {
      recentErrors.poll()
    }
  }
  
  /**
   * Get recent errors for this component
   */
  def getRecentErrors: Seq[AppError] = {
    import scala.jdk.CollectionConverters._
    recentErrors.asScala.toSeq
  }
  
  /**
   * Get error metrics for reporting
   */
  def getErrorMetrics: Map[String, Int] = {
    errorCounters.map { case (key, counter) => (key, counter.get()) }.toMap
  }
  
  /**
   * Reset error counters (e.g., after reporting)
   */
  def resetCounters(): Unit = {
    errorCounters.foreach { case (_, counter) => counter.set(0) }
  }
}

/**
 * Companion object for easier access to common error handling patterns
 */
object ErrorHandler {
  private val handlers = new TrieMap[String, ErrorHandler]()
  
  /**
   * Get or create an ErrorHandler for a specific component
   */
  def forComponent(componentName: String): ErrorHandler = {
    handlers.getOrElseUpdate(componentName, new ErrorHandler(componentName))
  }
  
  /**
   * Get all error metrics across all components
   */
  def getAllErrorMetrics: Map[String, Int] = {
    handlers.flatMap { case (_, handler) => handler.getErrorMetrics }.toMap
  }
  
  /**
   * Get all recent errors across all components
   */
  def getAllRecentErrors: Seq[AppError] = {
    handlers.flatMap { case (_, handler) => handler.getRecentErrors }.toSeq
  }
}