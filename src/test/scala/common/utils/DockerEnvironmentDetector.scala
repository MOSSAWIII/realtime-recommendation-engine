package common.utils

import java.io.File

object DockerEnvironmentDetector {
  // Check if running in Docker
  def isDockerEnvironment: Boolean = {
    // Multiple ways to detect Docker environment
    new File("/.dockerenv").exists() ||                     // Docker file check
    sys.env.contains("DOCKER_CONTAINER") ||                 // Environment variable
    sys.props.get("test.mode").contains("true") ||          // System property
    sys.env.get("TEST_MODE").exists(_.toLowerCase == "true") // Environment variable
  }
}

