package common.mocks

import com.dimafeng.testcontainers.SingleContainer
import org.testcontainers.containers.{RabbitMQContainer => JavaRabbitMQContainer}

class RabbitMQContainer(image: String = "rabbitmq:3-management") 
  extends SingleContainer[JavaRabbitMQContainer] {
  
  override val container: JavaRabbitMQContainer = new JavaRabbitMQContainer(image)
  
  def getHost: String = container.getHost
  def getAmqpPort: Int = container.getAmqpPort
  def getHttpPort: Int = container.getHttpPort
  def getAmqpUrl: String = container.getAmqpUrl
  def getMappedPort(port: Int): Int = container.getMappedPort(port)
}