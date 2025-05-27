package common.utils

import com.dimafeng.testcontainers.{ContainerDef, SingleContainer}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import scala.concurrent.duration._
import scala.util.Try

/**
 * Gestionnaire pour les containers de test
 */
object TestContainerManager {
  
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  private var containers = List.empty[SingleContainer[_]]
  
  /**
   * Démarre un container et l'enregistre pour nettoyage
   */
  def startContainer[T <: GenericContainer[_]](container: SingleContainer[T]): SingleContainer[T] = {
    Try {
      logger.info(s"Starting container: ${container.containerName}")
      container.start()
      containers = container :: containers
      container
    }.recover { case e =>
      logger.error(s"Failed to start container: ${e.getMessage}", e)
      throw e
    }.get
  }
  
  /**
   * Arrête tous les containers enregistrés
   */
  def stopAllContainers(): Unit = {
    containers.foreach { container =>
      Try {
        logger.info(s"Stopping container: ${container.containerName}")
        container.stop()
      }.recover { case e =>
        logger.error(s"Error stopping container: ${e.getMessage}")
      }
    }
    containers = List.empty
  }
  
  /**
   * Hook de fermeture de la JVM pour nettoyer les containers
   */
  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      stopAllContainers()
    }
  })
}