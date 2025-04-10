package repositories

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait BaseRepository {
  protected def executeWithErrorHandling[T](operation: => Future[T], 
                                           errorMsg: String)
                                          (implicit ec: ExecutionContext): Future[T] = {
    operation.recoverWith {
      case NonFatal(e) =>
        val errorDetails = s"$errorMsg: ${e.getMessage}"
        println(s"ERROR: $errorDetails")
        // Vous pourriez utiliser un logger ici
        Future.failed(new RuntimeException(errorDetails, e))
    }
  }
}

