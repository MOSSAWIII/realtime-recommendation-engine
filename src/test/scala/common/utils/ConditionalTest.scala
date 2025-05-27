// filepath: src/test/scala/common/utils/ConditionalTest.scala
package common.utils

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

/**
 * Trait pour les tests conditionnels qui s'exécutent uniquement dans certaines conditions
 */
trait ConditionalTest extends AnyFlatSpec with BeforeAndAfterAll {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Vérifier si les tests doivent être exécutés
   */
  def shouldRunTests: Boolean
  
  /**
   * Message expliquant pourquoi les tests sont ignorés
   */
  def skipMessage: String
  
  /**
   * Annuler tous les tests si la condition n'est pas remplie
   */
  override def run(testName: Option[String], args: org.scalatest.Args): org.scalatest.Status = {
    if (shouldRunTests) {
      super.run(testName, args)
    } else {
      logger.info(s"Skipping tests: $skipMessage")
      org.scalatest.SucceededStatus
    }
  }
}

/**
 * Test qui s'exécute uniquement si une propriété système est définie
 */
trait SystemPropertyConditionalTest extends ConditionalTest {
  val requiredProperty: String
  val requiredValue: Option[String] = None
  
  override def shouldRunTests: Boolean = {
    val propValue = Option(System.getProperty(requiredProperty))
    requiredValue match {
      case Some(value) => propValue.contains(value)
      case None => propValue.isDefined
    }
  }
  
  override def skipMessage: String = 
    requiredValue match {
      case Some(value) => s"System property '$requiredProperty' must be set to '$value'"
      case None => s"System property '$requiredProperty' must be defined"
    }
}

/**
 * Test qui s'exécute uniquement dans un environnement Docker
 */
trait DockerOnlyTest extends ConditionalTest {
  override def shouldRunTests: Boolean = DockerEnvironment.isDockerEnvironment
  
  override def skipMessage: String = "Test requires Docker environment"
}

/**
 * Test qui s'exécute uniquement si RabbitMQ est disponible
 */
trait RabbitMQConditionalTest extends ConditionalTest {
  private val rmqHost = DockerEnvironment.rmqHost
  private val rmqPort = DockerEnvironment.rmqPort.toInt
  
  override def shouldRunTests: Boolean = {
    import java.net.Socket
    try {
      val socket = new Socket(rmqHost, rmqPort)
      socket.close()
      true
    } catch {
      case _: Exception => false
    }
  }
  
  override def skipMessage: String = s"RabbitMQ not available at $rmqHost:$rmqPort"
}

