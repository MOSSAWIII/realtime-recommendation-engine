package utils.error

/**
 * Catégories d'erreurs pour faciliter le tri et la gestion des erreurs
 */
object ErrorCategory extends Enumeration {
  type ErrorCategory = Value
  
  // Définition des catégories d'erreurs
  val ValidationError = Value("ValidationError")
  val AuthenticationError = Value("AuthenticationError")
  val AuthorizationError = Value("AuthorizationError")
  val ResourceNotFoundError = Value("ResourceNotFoundError")
  val ConnectionError = Value("ConnectionError")
  val ProcessingError = Value("ProcessingError")
  val DataError = Value("DataError")
  val ConfigurationError = Value("ConfigurationError")
  val ExternalServiceError = Value("ExternalServiceError")
  val UnknownError = Value("UnknownError")
}