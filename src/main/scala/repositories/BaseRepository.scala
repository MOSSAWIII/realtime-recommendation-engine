package repositories

import scala.concurrent.{ExecutionContext, Future}
import org.slf4j.LoggerFactory
import scala.util.{Success, Failure, Try}

trait BaseRepository {
  protected val logger = LoggerFactory.getLogger(getClass)
  
  protected def executeWithErrorHandling[T](operation: => Future[T], errorMessage: String)
                                         (implicit ec: ExecutionContext): Future[T] = {
    val result = operation
    
    result.onComplete {
      case Success(_) => 
        logger.debug(s"Operation completed successfully: $errorMessage")
      case Failure(exception) =>
        logger.error(s"$errorMessage: ${exception.getMessage}", exception)
    }
    
    result.recoverWith { case e: Exception =>
      logger.error(s"$errorMessage: ${e.getMessage}", e)
      Future.failed(e)
    }
  }
  
  // Pour les opérations de retry
  protected def withRetry[T](operation: => Future[T], errorMessage: String, maxRetries: Int = 3)
                           (implicit ec: ExecutionContext): Future[T] = {
    def retry(n: Int): Future[T] = {
      operation.recoverWith {
        case e: Exception if n > 0 =>
          logger.warn(s"Retry $n/$maxRetries - $errorMessage: ${e.getMessage}")
          Thread.sleep(1000) // Attendre avant le prochain essai
          retry(n - 1)
        case e: Exception =>
          logger.error(s"Failed after $maxRetries attempts - $errorMessage: ${e.getMessage}", e)
          Future.failed(e)
      }
    }
    
    retry(maxRetries)
  }
}

