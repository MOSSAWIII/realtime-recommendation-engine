package integration.db

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import org.testcontainers.utility.DockerImageName
import common.base.IsolatedTestBase
import common.tags.TestTags
import common.utils.{TestResourceManager, TestInitializer}

trait PostgresIntegrationTestBase extends IsolatedTestBase with ForAllTestContainer {
  
  // Test ID unique pour ce test
  override val testId: String = TestResourceManager.uniqueId().toString
  
  // Définition du container PostgreSQL
  override val container: PostgreSQLContainer = PostgreSQLContainer(
    databaseName = s"test_db_${testId}",
    username = "postgres",
    password = "test_password"
  )
  
  /**
   * Retourne l'URL de connexion pour ce container
   */
  def getJdbcUrl: String = container.jdbcUrl
  
  /**
   * Crée une connexion à la base de données pour ce test
   */
  def createTestDbProvider(): infrastructure.config.DatabaseConfigProvider = {
    val configService = new common.mocks.TestConfigurationService(
      com.typesafe.config.ConfigFactory.parseString(
        s"""
        |database {
        |  url = "${container.jdbcUrl}"
        |  user = "${container.username}"
        |  password = "${container.password}"
        |  poolSize = 2
        |}
        """.stripMargin
      )
    )
    
    new infrastructure.config.DefaultDatabaseConfigProvider(configService)
  }
}