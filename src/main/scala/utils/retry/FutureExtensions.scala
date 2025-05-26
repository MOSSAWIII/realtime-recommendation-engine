package utils.retry

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}
import akka.actor.ActorSystem

/**
 * Extensions pour les Futures pour ajouter des fonctionnalités utiles
 */
object FutureExtensions {
  // Implicit ActorSystem for timeouts with explicit type
  private implicit val system: ActorSystem = ActorSystem("FutureExtensions")
  
  implicit class FutureOps[T](future: Future[T]) {
    def withTimeout(duration: FiniteDuration)(implicit ec: ExecutionContext): Future[T] = {
      val promise = Promise[T]()
      val timeout = akka.pattern.after(duration) {
        Future.failed(new java.util.concurrent.TimeoutException(s"Future timed out after $duration"))
      }
      
      promise.completeWith(Future.firstCompletedOf(Seq(future, timeout)))
      promise.future
    }
  }
}