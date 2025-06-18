package common.base

import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Trait for tests that should only run under certain conditions
 */
trait ConditionalTest extends BeforeAndAfterAll { this: Suite =>
  def shouldRunTests: Boolean
  def skipMessage: String
  
  abstract override def beforeAll(): Unit = {
    if (!shouldRunTests) {
      cancel(skipMessage)
    }
    super.beforeAll()
  }
}

/**
 * Trait for tests that should only run in Docker environment
 */
trait DockerOnlyTest extends ConditionalTest { this: Suite =>
  // Implement the required methods instead of trying to override a non-existent method
  def shouldRunTests: Boolean = {
    val isDockerEnv = Option(System.getenv("TEST_MODE")).exists(_.toLowerCase == "true") ||
                      sys.props.get("test.mode").exists(_.toLowerCase == "true")
    
    if (!isDockerEnv) {
      cancel("Test requires Docker environment")
    }
    true
  }
  
  def skipMessage: String = "Test requires Docker environment"
}

