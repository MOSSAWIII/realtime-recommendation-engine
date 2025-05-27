package common.utils

import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.utility.DockerImageName
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import java.util.Properties
import scala.util.Try
import scala.jdk.CollectionConverters._
import org.slf4j.LoggerFactory

/**
 * Configuration pour les TestContainers
 */
object TestContainerConfig {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Get test run ID from environment or system property
  lazy val testRunId: String = Option(System.getenv("TEST_RUN_ID"))
    .orElse(Option(System.getProperty("test.runId")))
    .getOrElse(TestResourceManager.uniqueId().toString)
  
  // Détection si nous devons utiliser TestContainers ou des services externes
  lazy val useExternalServices: Boolean = Option(System.getenv("TEST_USE_EXTERNAL")).exists(_.toLowerCase == "true")
  
  // Constructeur de PostgreSQL avec options spécifiques
  def createPostgresContainer(dbName: String = s"test_db_${testRunId}"): PostgreSQLContainer[_] = {
    val container = new PostgreSQLContainer(
      DockerImageName.parse("postgres:14").asCompatibleSubstituteFor("postgres")
    )
    container.withDatabaseName(dbName)
    container.withUsername("postgres")
    container.withPassword("test_password")
    container.withReuse(true) // Réutiliser les conteneurs pour plus de rapidité
    
    container
  }
  
  // Constructeur de RabbitMQ avec options spécifiques
  def createRabbitMQContainer(): RabbitMQContainer = {
    val container = new RabbitMQContainer(
      DockerImageName.parse("rabbitmq:3-management")
    )
    container.withReuse(true)
    
    // Set unique names for exchanges/queues
    val env = new Properties()
    env.put("RABBITMQ_DEFAULT_EXCHANGE", s"test_exchange_${testRunId}")
    container.withEnv(env.asScala.toMap.asJava)
    
    container
  }
  
  // Obtient la configuration pour les tests avec TestContainers
  def getContainerizedConfig(pgContainer: Option[PostgreSQLContainer[_]] = None, 
                             rmqContainer: Option[RabbitMQContainer] = None): Config = {
    val baseConfig = ConfigFactory.load("application-test.conf")
    
    // Nouvelle configuration avec valeurs provenant des conteneurs
    val configWithContainers = baseConfig
      .withValue("test.runId", ConfigValueFactory.fromAnyRef(testRunId))
      
    // Ajouter la configuration PostgreSQL si disponible
    val configWithPg = pgContainer.fold(configWithContainers) { pg =>
      configWithContainers
        .withValue("database.url", ConfigValueFactory.fromAnyRef(pg.getJdbcUrl))
        .withValue("database.user", ConfigValueFactory.fromAnyRef(pg.getUsername))
        .withValue("database.password", ConfigValueFactory.fromAnyRef(pg.getPassword))
    }
    
    // Ajouter la configuration RabbitMQ si disponible
    val configWithRmq = rmqContainer.fold(configWithPg) { rmq =>
      configWithPg
        .withValue("rabbitmq.host", ConfigValueFactory.fromAnyRef(rmq.getHost))
        .withValue("rabbitmq.port", ConfigValueFactory.fromAnyRef(rmq.getAmqpPort.toString))
        .withValue("rabbitmq.username", ConfigValueFactory.fromAnyRef("guest"))
        .withValue("rabbitmq.password", ConfigValueFactory.fromAnyRef("guest"))
        .withValue("rabbitmq.exchange.name", 
                  ConfigValueFactory.fromAnyRef(s"user_actions_exchange_${testRunId}"))
    }
    
    configWithRmq
  }
  
  // Récupère la configuration depuis les variables d'environnement Docker
  def getExternalServicesConfig(): Config = {
    val baseConfig = ConfigFactory.load("application-test.conf")
    
    // Use environment variables for external services
    val dbHost = Option(System.getenv("DATABASE_HOST")).getOrElse("test-db")
    val dbPort = Option(System.getenv("DATABASE_PORT")).getOrElse("5432")
    val dbName = Option(System.getenv("DATABASE_NAME")).getOrElse("test_recommendation_engine")
    val dbUser = Option(System.getenv("DATABASE_USER")).getOrElse("postgres")
    val dbPassword = Option(System.getenv("DATABASE_PASSWORD")).getOrElse("test_password")
    
    val rmqHost = Option(System.getenv("RABBITMQ_HOST")).getOrElse("test-rabbitmq")
    val rmqPort = Option(System.getenv("RABBITMQ_PORT")).getOrElse("5672")
    val rmqUser = Option(System.getenv("RABBITMQ_USERNAME")).getOrElse("guest")
    val rmqPassword = Option(System.getenv("RABBITMQ_PASSWORD")).getOrElse("guest")
    val rmqExchange = Option(System.getenv("TEST_EXCHANGE"))
      .getOrElse(s"user_actions_exchange_${testRunId}")
    
    baseConfig
      .withValue("test.runId", ConfigValueFactory.fromAnyRef(testRunId))
      .withValue("database.url", 
                ConfigValueFactory.fromAnyRef(s"jdbc:postgresql://$dbHost:$dbPort/$dbName"))
      .withValue("database.user", ConfigValueFactory.fromAnyRef(dbUser))
      .withValue("database.password", ConfigValueFactory.fromAnyRef(dbPassword))
      .withValue("rabbitmq.host", ConfigValueFactory.fromAnyRef(rmqHost))
      .withValue("rabbitmq.port", ConfigValueFactory.fromAnyRef(rmqPort))
      .withValue("rabbitmq.username", ConfigValueFactory.fromAnyRef(rmqUser))
      .withValue("rabbitmq.password", ConfigValueFactory.fromAnyRef(rmqPassword))
      .withValue("rabbitmq.exchange.name", ConfigValueFactory.fromAnyRef(rmqExchange))
  }
  
  // Récupère la configuration appropriée selon le contexte
  def getTestConfig(pgContainer: Option[PostgreSQLContainer[_]] = None, 
                    rmqContainer: Option[RabbitMQContainer] = None): Config = {
    if (useExternalServices) {
      logger.info("Using external services for tests")
      getExternalServicesConfig()
    } else if (pgContainer.isDefined || rmqContainer.isDefined) {
      logger.info("Using TestContainers for tests")
      getContainerizedConfig(pgContainer, rmqContainer)
    } else {
      logger.info("Using default test configuration")
      ConfigFactory.load("application-test.conf")
        .withValue("test.runId", ConfigValueFactory.fromAnyRef(testRunId))
    }
  }
}