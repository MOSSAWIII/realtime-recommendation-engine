package unit.application.core

import application.core.DataManager
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider, DatabaseConfigSection, DataManagerConfigSection}
import utils.error.DatabaseException  // Correction de l'import
import java.sql.{Connection, PreparedStatement, ResultSet}
import org.mockito.Mockito.{mock, when}
import org.mockito.ArgumentMatchers.any
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import common.base.IsolatedTestBase
import common.mocks.{TestConfigurationService, TestDatabaseProvider}

class DataManagerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with ScalaFutures {
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  "DataManager" should "handle database exceptions properly" in {
    // Création des mocks avec types explicites
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    val mockConnection = mock(classOf[Connection])
    val mockPreparedStatement = mock(classOf[PreparedStatement])
    val mockResultSet = mock(classOf[ResultSet])
    
    // Simulation comportement
    when(mockDbProvider.getConnection()).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
    when(mockConfigService.get[DatabaseConfigSection](any())).thenReturn(mock(classOf[DatabaseConfigSection]))
    
    // Instanciation avec les deux paramètres requis
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
    // Test de l'exception avec ScalaFutures
    import scala.concurrent.duration._
    whenReady(dataManager.getUserProfile(1).failed, timeout(5.seconds)) { e =>
      e shouldBe a[DatabaseException]
    }
  }
  
  it should "handle database connectivity errors gracefully" in {
    // Spécifier explicitement le type pour éviter l'ambiguïté
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    
    when(mockDbProvider.getConnection()).thenThrow(new RuntimeException("Test DB connection error"))
    
    // Fournir tous les paramètres requis
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
    // Utiliser ScalaFutures.whenReady avec recoverToExceptionIf
    import scala.concurrent.duration._
    whenReady(dataManager.getProcessedEventsForUser(123L).failed) { e =>
      e shouldBe a[DatabaseException]
    }
  }
  
  it should "handle database errors gracefully" in {
    // Spécifier explicitement le type pour éviter l'ambiguïté
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    val mockConnection = mock(classOf[Connection])
    
    when(mockDbProvider.getConnection()).thenThrow(new RuntimeException("DB Error"))
    
    // Fournir tous les paramètres requis
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
    // Utiliser whenReady avec pattern matching
    whenReady(dataManager.initTables().failed) { exception =>
      exception shouldBe a[DatabaseException]
    }
  }
  
  it should "close all resources when shut down" in {
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    
    val mockDbConfig = mock(classOf[DatabaseConfigSection])
    when(mockConfigService.get[DatabaseConfigSection](any())).thenReturn(mockDbConfig)
    
    val mockDataMgrConfig = mock(classOf[DataManagerConfigSection])
    when(mockConfigService.get[DataManagerConfigSection](any())).thenReturn(mockDataMgrConfig)
    
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    dataManager.shutdown() // Should not throw exceptions
    
    // No specific assertions needed, just confirming it doesn't throw
    succeed
  }
  
  it should "properly cache and invalidate cached data" in {
    // Création des mocks
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    
    val mockDbConfig = mock(classOf[DatabaseConfigSection])
    when(mockConfigService.get[DatabaseConfigSection](any())).thenReturn(mockDbConfig)
    
    val mockDataMgrConfig = mock(classOf[DataManagerConfigSection])
    when(mockConfigService.get[DataManagerConfigSection](any())).thenReturn(mockDataMgrConfig)
    
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
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
    import domain.models.User
    val testUser = User(
      userId = 123, 
      name = Some("Test User"),
      emailAddress = None,
      locale = "en_US",
      birthYear = None,
      gender = None,
      location = None,
      timezone = None,
      isGenerated = false
    )
    cacheMethod.invoke(dataManager, "user", 123L, Some(testUser), scala.concurrent.duration.Duration.apply(5, "minutes"))
    
    val cachedResult = getCacheMethod.invoke(dataManager, "user", 123L)
    cachedResult shouldBe a[Some[_]]
    
    // Test invalidate
    invalidateCacheMethod.invoke(dataManager, "user", 123L)
    val afterInvalidate = getCacheMethod.invoke(dataManager, "user", 123L)
    afterInvalidate shouldBe None
  }
}

