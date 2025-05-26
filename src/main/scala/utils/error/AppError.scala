package utils.error

import utils.error.ErrorCategory.ErrorCategory

/**
 * Classe de base pour les erreurs applicatives avec catégorisation
 */
case class AppError(
  message: String,
  cause: Option[Throwable] = None,
  category: ErrorCategory = ErrorCategory.UnknownError,
  context: Map[String, String] = Map.empty
) extends Exception(message, cause.orNull) {
  
  /**
   * Ajoute des informations contextuelles à l'erreur
   */
  def withContext(key: String, value: String): AppError = 
    this.copy(context = context + (key -> value))
    
  /**
   * Ajoute plusieurs informations contextuelles à l'erreur
   */
  def withContext(additionalContext: Map[String, String]): AppError =
    this.copy(context = context ++ additionalContext)
}