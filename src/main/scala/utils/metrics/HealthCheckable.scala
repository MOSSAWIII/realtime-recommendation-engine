package utils.metrics

trait HealthCheckable {
  /**
   * Perform a health check on the component
   * @return true if the component is healthy, false otherwise
   */
  def healthCheck(): Boolean
  
  /**
   * Get detailed health check information
   * @return Map of component name to health status and details
   */
  def healthStatus(): Map[String, Map[String, Any]] = Map.empty
}

