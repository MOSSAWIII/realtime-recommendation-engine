package common.mocks

import com.dimafeng.testcontainers.SingleContainer
import org.testcontainers.containers.{RabbitMQContainer => JavaRabbitMQContainer}

class RabbitMQContainer(dockerImageName: String = "rabbitmq:3-management") 
  extends SingleContainer[JavaRabbitMQContainer] {
  
  override val container: JavaRabbitMQContainer = new JavaRabbitMQContainer(dockerImageName)
  
  def getHost: String = container.getHost
  def getAmqpPort: Int = container.getAmqpPort
  def getHttpPort: Int = container.getHttpPort
  def getMappedPort(port: Int): Int = container.getMappedPort(port)
}