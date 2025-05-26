package common.utils

import com.dimafeng.testcontainers.{GenericContainer}
import org.testcontainers.containers.{RabbitMQContainer => JavaRabbitMQContainer}

// Wrapper pour RabbitMQContainer de TestContainers
class RabbitMQContainer(imageName: String) extends GenericContainer(
  new JavaRabbitMQContainer(imageName)
) {
  // Accéder au conteneur sous-jacent via la méthode container disponible dans GenericContainer
  def getAmqpUrl: String = container.asInstanceOf[JavaRabbitMQContainer].getAmqpUrl
  def getAdminUsername: String = container.asInstanceOf[JavaRabbitMQContainer].getAdminUsername
  def getAdminPassword: String = container.asInstanceOf[JavaRabbitMQContainer].getAdminPassword
  def getMappedPort(port: Int): Int = container.asInstanceOf[JavaRabbitMQContainer].getMappedPort(port)
}