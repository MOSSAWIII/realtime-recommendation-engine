package utils.error

/**
 * Exception class for database-related errors.
 */
class DatabaseException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

