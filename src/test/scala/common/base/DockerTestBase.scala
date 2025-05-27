package common.base

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer, MultipleContainers}
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import org.testcontainers.containers.{PostgreSQLContainer => JavaPostgreSQLContainer}
import org.testcontainers.utility.DockerImageName
import org.slf4j.LoggerFactory
import com.typesafe.config.Config

/**
 * Base trait pour les tests nécessitant des conteneurs Docker
 */
trait DockerTestBase extends IsolatedTestBase with ForAllTestContainer {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Correction de la création du container PostgreSQL
  val pgContainer = PostgreSQLContainer(
    DockerImageName.parse("postgres:14").toString(),
    databaseName = "test_db",
    username = "postgres",
    password = "test_password"
  )
  
  // Utilisation d'un wrapper pour le conteneur RabbitMQ
  val rabbitMQContainer = new com.dimafeng.testcontainers.GenericContainer(
    "rabbitmq:3-management",
    exposedPorts = Seq(5672, 15672),
    env = Map("RABBITMQ_DEFAULT_USER" -> "guest", "RABBITMQ_DEFAULT_PASS" -> "guest")
  )
  
  // Utilisation de MultipleContainers avec des types compatibles
  override val container = MultipleContainers(pgContainer, rabbitMQContainer)
  
  // Accesseurs pour les conteneurs individuels - nouvelle implémentation sans containersRegistry
  def getPostgreSQLContainer: PostgreSQLContainer = pgContainer
  def getRabbitMQContainer: com.dimafeng.testcontainers.GenericContainer = rabbitMQContainer
  
  // Fonctions utilitaires pour obtenir les configurations de connexion
  def getJdbcUrl: String = pgContainer.jdbcUrl
  def getDbUsername: String = pgContainer.username
  def getDbPassword: String = pgContainer.password
  
  def getRabbitMQHost: String = rabbitMQContainer.container.getHost
  def getRabbitMQPort: Int = rabbitMQContainer.container.getMappedPort(5672)
}

