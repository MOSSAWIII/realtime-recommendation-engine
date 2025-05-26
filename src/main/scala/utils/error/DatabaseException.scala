package utils.error

/**
 * Exception thrown for database related errors
 */
class DatabaseException(
  message: String, 
  cause: Throwable = null,
  val details: Map[String, String] = Map.empty
) extends RuntimeException(message, cause) {
  
  /**
   * Get a detail value by key
   */
  def getDetail(key: String): Option[String] = details.get(key)
  
  /**
   * Check if a specific detail exists
   */
  def hasDetail(key: String): Boolean = details.contains(key)
  
  /**
   * Get all details as a map
   */
  def getAllDetails: Map[String, String] = details
}

object DatabaseException {
  def apply(message: String, cause: Throwable = null, details: Map[String, String] = Map.empty): DatabaseException = 
    new DatabaseException(message, cause, details)
}

