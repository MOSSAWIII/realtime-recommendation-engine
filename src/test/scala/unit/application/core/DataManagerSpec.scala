package unit.application.core

import application.core.DataManager
import common.base.UnitSpec
import infrastructure.config.{ConfigurationService, DatabaseConfigSection, DataManagerConfigSection}
import com.typesafe.config.ConfigFactory
import domain.models._
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import utils.error.DatabaseException

class DataManagerSpec extends UnitSpec with Matchers {
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  "DataManager" should "handle database connectivity errors gracefully" in {
    // Mock configuration service that returns valid-looking but non-functional DB config
    val mockConfig = createMockConfig("jdbc:postgresql://non-existent-host:5432/test", "user", "pass")
    
    val dataManager = new DataManager(mockConfig)
    
    // Test connection should fail, but not throw exception
    val thrown = intercept[DatabaseException] {
      Await.result(dataManager.getUserProfile(1L), 5.seconds)
    }
    
    thrown.getMessage should include("Failed to acquire database connection")
    
    dataManager.close()
  }
  
  it should "close all resources when shut down" in {
    val mockConfig = createMockConfig("jdbc:postgresql://non-existent-host:5432/test", "user", "pass")
    
    val dataManager = new DataManager(mockConfig)
    dataManager.start() // Start even though it will fail to connect
    dataManager.close() // Should not throw exceptions
    
    // No specific assertions needed, just confirming it doesn't throw
    succeed
  }
  
  it should "properly cache and invalidate cached data" in {
    // This test requires mocking the internal cache behavior
    // We'll use reflection to access the private cache
    
    val mockConfig = createMockConfig("jdbc:postgresql://localhost:5432/test", "user", "pass")
    val dataManager = new DataManager(mockConfig)
    
    // Create a test for caching using reflection
    val cacheMethod = dataManager.getClass.getDeclaredMethod("putInCache", 
      classOf[String], classOf[Any], classOf[Any], classOf[scala.concurrent.duration.Duration])
    cacheMethod.setAccessible(true)
    
    val getCacheMethod = dataManager.getClass.getDeclaredMethod("getFromCache", 
      classOf[String], classOf[Any])
    getCacheMethod.setAccessible(true)
    
    val invalidateCacheMethod = dataManager.getClass.getDeclaredMethod("invalidateCache", 
      classOf[String], classOf[Any])
    invalidateCacheMethod.setAccessible(true)
    
    // Test put and get
    val testUser = User(userId = 123, name = Some("Test User"))
    cacheMethod.invoke(dataManager, "user", 123L, Some(testUser), scala.concurrent.duration.Duration.apply(5, "minutes"))
    
    val cachedResult = getCacheMethod.invoke(dataManager, "user", 123L)
    cachedResult shouldBe a[Some[_]]
    
    // Test invalidate
    invalidateCacheMethod.invoke(dataManager, "user", 123L)
    val afterInvalidate = getCacheMethod.invoke(dataManager, "user", 123L)
    afterInvalidate shouldBe None
  }
  
  private def createMockConfig(url: String, user: String, password: String): ConfigurationService = {
    val configService = mock(classOf[ConfigurationService])
    
    // Create mock DB config
    val mockDbConfig = new DatabaseConfigSection(ConfigFactory.parseString(s"""
      |database {
      |  url = "$url"
      |  user = "$user" 
      |  password = "$password"
      |  poolSize = 5
      |  connectionTimeout = "5s"
      |  idleTimeout = "10m"
      |}
    """.stripMargin))
    
    // Create mock DataManager config
    val mockDataMgrConfig = new DataManagerConfigSection(ConfigFactory.parseString(s"""
      |dataManager {
      |  cache {
      |    defaultTTL = "5m"
      |    shortTTL = "1m"
      |  }
      |  query {
      |    timeout = "5s"
      |    maxPoolSize = 5
      |    defaultBatchSize = 100
      |  }
      |  transaction {
      |    timeout = "5s"
      |    maxRetries = 3 
      |    retryDelay = "1s"
      |  }
      |}
    """.stripMargin))
    
    when(configService.get[DatabaseConfigSection]).thenReturn(mockDbConfig)
    when(configService.get[DataManagerConfigSection]).thenReturn(mockDataMgrConfig)
    
    configService
  }
}

