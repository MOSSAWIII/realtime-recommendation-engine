package utils.retry

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}
import utils.error.ErrorCategory
import utils.error.ErrorCategory._
import utils.error.{AppError, ErrorHandler}
import org.slf4j.LoggerFactory
import utils.retry.FutureExtensions._
import akka.actor.ActorSystem
import java.util.concurrent.atomic.AtomicInteger

/**
 * Politique de retry configurable pour les opérations qui peuvent échouer temporairement
 */
case class RetryConfig(
  maxRetries: Int = 3,
  initialDelay: FiniteDuration,
  maxDelay: FiniteDuration,
  backoffFactor: Double = 2.0,
  timeout: FiniteDuration,
  retriableCategories: Set[ErrorCategory] = Set(ConnectionError, ProcessingError, ExternalServiceError)
)

/**
 * Implémentation d'une politique de retry avec backoff exponentiel pour des opérations synchrones
 */
class SyncRetryPolicy(
  component: String,
  config: RetryConfig
) {
  private val logger = LoggerFactory.getLogger(s"RetryPolicy-$component")
  
  /**
   * Exécute une opération avec retry
   *
   * @param operation Opération à exécuter
   * @param onRetry Callback appelé à chaque retry
   * @return Résultat de l'opération ou lève une exception après tous les retries
   */
  def execute[T](operation: => T, onRetry: (Throwable, Int, FiniteDuration) => Unit = defaultOnRetry): T = {
    var currentDelay = config.initialDelay
    var attempt = 0
    val maxAttempts = config.maxRetries + 1 // +1 pour l'essai initial
    
    while (true) {
      try {
        // Tentative d'exécution
        val result = operation
        return result
      } catch {
        case e: Throwable =>
          attempt += 1
          
          // Vérifier si on doit faire un retry
          if (attempt >= maxAttempts || !isRetriable(e)) {
            logger.error(s"[$component] Operation failed after $attempt attempts", e)
            throw e
          }
          
          // Calculer le délai pour le prochain retry (backoff exponentiel)
          currentDelay = calculateNextDelay(currentDelay)
          
          // Notifier du retry
          onRetry(e, attempt, currentDelay)
          
          // Attendre avant le retry
          Thread.sleep(currentDelay.toMillis)
      }
    }
    
    throw new IllegalStateException("Should never reach this code")
  }
  
  private val errorHandler = ErrorHandler.forComponent(component)
  
  private def defaultOnRetry(error: Throwable, attempt: Int, delay: FiniteDuration): Unit = {
    logger.warn(s"[$component] Retry attempt $attempt after ${delay.toMillis}ms, reason: ${error.getMessage}")
  }
  
  private def calculateNextDelay(currentDelay: FiniteDuration): FiniteDuration = {
    val nextDelay = (currentDelay.toMillis * config.backoffFactor).toLong
    Duration.fromNanos(Math.min(nextDelay, config.maxDelay.toNanos))
      .asInstanceOf[FiniteDuration]
  }
  
  private def isRetriable(error: Throwable): Boolean = {
    error match {
      case appError: AppError => 
        config.retriableCategories.contains(appError.category)
      case _ => 
        // Par défaut, certaines exceptions sont considérées comme retriables
        error match {
          case _: java.net.ConnectException => true
          case _: java.io.IOException => true
          case _: java.sql.SQLException => true
          case _: java.util.concurrent.TimeoutException => true
          case _ => false
        }
    }
  }
}

/**
 * Implémentation d'une politique de retry avec backoff exponentiel pour des opérations asynchrones
 */
class AsyncRetryPolicy(
  component: String,
  config: RetryConfig
)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(s"AsyncRetryPolicy-$component")
  
  // Ajout d'un ActorSystem explicite pour after()
  private implicit val system: ActorSystem = ActorSystem(s"RetryPolicy-$component")
  
  /**
   * Exécute une opération asynchrone avec retry
   *
   * @param operation Opération asynchrone à exécuter
   * @param onRetry Callback appelé à chaque retry
   * @return Future avec le résultat ou échec après tous les retries
   */
  def executeAsync[T](
    operation: => Future[T],
    onRetry: (Throwable, Int, FiniteDuration) => Unit = defaultOnRetry
  ): Future[T] = {
    val promise = Promise[T]()
    executeWithPromise(promise, operation, 0, config.initialDelay, onRetry)
    
    // Ajouter un timeout global
    val timeoutFuture = promise.future.withTimeout(config.timeout)
    
    timeoutFuture.recoverWith {
      case e: java.util.concurrent.TimeoutException =>
        Future.failed(
          errorHandler.asAppError(
            e, 
            ErrorCategory.ProcessingError
          ).withContext("component", component)
           .withContext("operation", "asyncRetry")
           .withContext("timeout", config.timeout.toString)
        )
    }
  }
  
  private val errorHandler = ErrorHandler.forComponent(component)
  
  private def executeWithPromise[T](
    promise: Promise[T],
    operation: => Future[T],
    attempt: Int,
    delay: FiniteDuration,
    onRetry: (Throwable, Int, FiniteDuration) => Unit
  ): Unit = {
    // Exécuter l'opération
    val future = try {
      operation
    } catch {
      case e: Throwable =>
        Future.failed(e)
    }
    
    future.onComplete {
      case Success(result) =>
        // Succès - compléter la promesse
        promise.trySuccess(result)
        
      case Failure(error) =>
        val nextAttempt = attempt + 1
        
        // Vérifier si on doit faire un retry
        if (nextAttempt > config.maxRetries || !isRetriable(error)) {
          logger.error(s"[$component] Async operation failed after ${nextAttempt} attempts", error)
          promise.tryFailure(error)
          return
        }
        
        // Calculer le délai pour le prochain retry
        val nextDelay = calculateNextDelay(delay)
        
        // Notifier du retry
        onRetry(error, nextAttempt, nextDelay)
        
        // Planifier le prochain retry après le délai
        akka.pattern.after(nextDelay) {
          Future {
            executeWithPromise(promise, operation, nextAttempt, nextDelay, onRetry)
          }
        }
    }
  }
  
  private def defaultOnRetry(error: Throwable, attempt: Int, delay: FiniteDuration): Unit = {
    logger.warn(s"[$component] Async retry attempt $attempt after ${delay.toMillis}ms, reason: ${error.getMessage}")
  }
  
  private def calculateNextDelay(currentDelay: FiniteDuration): FiniteDuration = {
    val nextDelay = (currentDelay.toMillis * config.backoffFactor).toLong
    Duration.fromNanos(Math.min(nextDelay, config.maxDelay.toNanos))
      .asInstanceOf[FiniteDuration]
  }
  
  private def isRetriable(error: AppError): Boolean = {
    config.retriableCategories.contains(error.category)
  }
  
  private def isRetriable(error: Throwable): Boolean = {
    error match {
      case appError: AppError => 
        isRetriable(appError)
      case _ => 
        // Par défaut, certaines exceptions sont considérées comme retriables
        error match {
          case _: java.net.ConnectException => true
          case _: java.io.IOException => true
          case _: java.sql.SQLException => true
          case _: java.util.concurrent.TimeoutException => true
          case _ => false
        }
    }
  }
}

/**
 * Factory pour créer facilement des politiques de retry
 */
object RetryPolicy {
  def sync(component: String, config: RetryConfig): SyncRetryPolicy = 
    new SyncRetryPolicy(component, config)
    
  def async(component: String, config: RetryConfig)(implicit ec: ExecutionContext): AsyncRetryPolicy = 
    new AsyncRetryPolicy(component, config)(ec)
}