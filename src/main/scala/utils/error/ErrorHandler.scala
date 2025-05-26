package utils.error

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import infrastructure.config.{ConfigurationService, ErrorConfigSection}
import utils.error.ErrorCategory.ErrorCategory
import org.slf4j.{Logger, LoggerFactory}

/**
 * Gestionnaire centralisé d'erreurs pour une manipulation cohérente
 * des erreurs à travers l'application
 */
class ErrorHandler(componentName: String) {
  private val logger = LoggerFactory.getLogger(s"ErrorHandler-$componentName")
  private val errorConfig = ConfigurationService.getInstance.getConfigSection(new ErrorConfigSection(_))
  
  // File circulaire des erreurs récentes
  private val recentErrors = mutable.Queue[AppError]()
  private val maxRecentErrors = errorConfig.maxRecentErrors
  
  /**
   * Enregistre et traite une erreur
   */
  def handleError(error: AppError): AppError = {
    // Journalisation avec niveau approprié selon la catégorie
    logError(error)
    
    // Ajouter à la file des erreurs récentes
    synchronized {
      if (recentErrors.size >= maxRecentErrors) {
        recentErrors.dequeue()
      }
      recentErrors.enqueue(error)
    }
    
    // Signaler l'erreur si la configuration le permet
    if (errorConfig.reportingEnabled) {
      reportError(error)
    }
    
    error
  }
  
  /**
   * Journalise l'erreur avec le niveau approprié
   */
  private def logError(error: AppError): Unit = {
    val errorMsg = buildErrorMessage(error)
    
    error.category match {
      case ErrorCategory.ValidationError | ErrorCategory.ResourceNotFoundError =>
        logger.warn(errorMsg, error.cause.orNull)
      case _ => 
        logger.error(errorMsg, error.cause.orNull)
    }
  }
  
  /**
   * Construit un message d'erreur formaté
   */
  private def buildErrorMessage(error: AppError): String = {
    val sb = new StringBuilder(s"[$componentName] [${error.category}] ${error.message}")
    
    if (error.context.nonEmpty && errorConfig.enableEnhancedLogging) {
      sb.append(" | Context: ")
      sb.append(error.context.map { case (k, v) => s"$k=$v" }.mkString(", "))
    }
    
    sb.toString
  }
  
  /**
   * Signale l'erreur à un service externe (monitoring, etc.)
   */
  private def reportError(error: AppError): Unit = {
    // Implémentation du reporting - pour l'instant juste un placeholder
    // Pourrait envoyer à un service comme Sentry, New Relic, etc.
  }
  
  /**
   * Convertit une Exception en AppError
   */
  def asAppError(throwable: Throwable, category: ErrorCategory = ErrorCategory.UnknownError): AppError = {
    throwable match {
      case appError: AppError => appError
      case other => AppError(
        message = Option(other.getMessage).getOrElse(other.getClass.getSimpleName),
        cause = Some(other),
        category = category
      )
    }
  }
  
  /**
   * Version Future de handleError pour gérer les erreurs dans des contextes asynchrones
   */
  def handleErrorF[T](future: Future[T])(implicit ec: ExecutionContext): Future[T] = {
    future.recoverWith {
      case appError: AppError =>
        Future.failed(handleError(appError))
      case throwable: Throwable =>
        Future.failed(handleError(asAppError(throwable)))
    }
  }
}

object ErrorHandler {
  private val handlers = mutable.Map[String, ErrorHandler]()
  
  /**
   * Obtient ou crée un gestionnaire d'erreurs pour un composant spécifique
   */
  def forComponent(componentName: String): ErrorHandler = synchronized {
    handlers.getOrElseUpdate(componentName, new ErrorHandler(componentName))
  }
}