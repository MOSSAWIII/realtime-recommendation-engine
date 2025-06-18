package unit.application.core

import application.core.DataManager
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider, DatabaseConfigSection, DataManagerConfigSection}
import utils.error.DatabaseException
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
    
    // Configuration des mocks pour simuler une exception SQL
    when(mockDbProvider.getConnection()).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeQuery()).thenThrow(new java.sql.SQLException("Test SQL exception"))
    
    // Configuration des sections de config
    val mockDbConfig = mock(classOf[DatabaseConfigSection])
    when(mockConfigService.get[DatabaseConfigSection](any())).thenReturn(mockDbConfig)
    
    val mockDataMgrConfig = mock(classOf[DataManagerConfigSection])
    when(mockConfigService.get[DataManagerConfigSection](any())).thenReturn(mockDataMgrConfig)
    
    // Instanciation avec les deux paramètres requis
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
    // Test de l'exception avec ScalaFutures - correction: utiliser recoverToSucceededIf
    import scala.concurrent.duration._
    whenReady(dataManager.getUserProfile(1).failed, timeout(5.seconds)) { e =>
      e shouldBe a[DatabaseException]
    }
  }
  
  it should "handle database connectivity errors gracefully" in {
    // Spécifier explicitement le type pour éviter l'ambiguïté
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    
    // Simuler une erreur de connexion BD
    when(mockDbProvider.getConnection()).thenThrow(new RuntimeException("Test DB connection error"))
    
    // Configuration des sections de config
    val mockDbConfig = mock(classOf[DatabaseConfigSection])
    when(mockConfigService.get[DatabaseConfigSection](any())).thenReturn(mockDbConfig)
    
    val mockDataMgrConfig = mock(classOf[DataManagerConfigSection])
    when(mockConfigService.get[DataManagerConfigSection](any())).thenReturn(mockDataMgrConfig)
    
    // Fournir tous les paramètres requis
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
    // Utiliser ScalaFutures.whenReady avec recoverToExceptionIf - correction: tester que c'est bien une DatabaseException
    import scala.concurrent.duration._
    whenReady(dataManager.getProcessedEventsForUser(123L).failed, timeout(5.seconds)) { e =>
      e shouldBe a[DatabaseException]
      e.getMessage should include("Test DB connection error")
    }
  }
  
  it should "handle database errors gracefully" in {
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    
    // Configuration des sections de config
    val mockDbConfig = mock(classOf[DatabaseConfigSection])
    when(mockConfigService.get[DatabaseConfigSection](any())).thenReturn(mockDbConfig)
    
    val mockDataMgrConfig = mock(classOf[DataManagerConfigSection])
    when(mockConfigService.get[DataManagerConfigSection](any())).thenReturn(mockDataMgrConfig)
    
    when(mockDbProvider.getConnection()).thenThrow(new RuntimeException("DB Error"))
    
    // Fournir tous les paramètres requis
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
    // Utiliser whenReady avec pattern matching - correction: tester initTables au lieu de getProcessedEventsForUser
    whenReady(dataManager.initTables().failed, timeout(5.seconds)) { exception =>
      exception shouldBe a[DatabaseException]
      exception.getMessage should include("DB Error")
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
  
  it should "handle successful database operations" in {
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    val mockConnection = mock(classOf[Connection])
    val mockPreparedStatement = mock(classOf[PreparedStatement])
    val mockResultSet = mock(classOf[ResultSet])
    
    // Configuration des sections de config
    val mockDbConfig = mock(classOf[DatabaseConfigSection])
    when(mockConfigService.get[DatabaseConfigSection](any())).thenReturn(mockDbConfig)
    
    val mockDataMgrConfig = mock(classOf[DataManagerConfigSection])
    when(mockConfigService.get[DataManagerConfigSection](any())).thenReturn(mockDataMgrConfig)
    
    // Simuler une opération réussie
    when(mockDbProvider.getConnection()).thenReturn(mockConnection)
    when(mockConnection.prepareStatement(any[String])).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
    when(mockResultSet.next()).thenReturn(false) // Pas de résultats
    
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
    // Test d'une opération qui devrait réussir
    whenReady(dataManager.getUserProfile(1), timeout(5.seconds)) { result =>
      result shouldBe None // Pas d'utilisateur trouvé, mais pas d'exception
    }
  }
  
  it should "pass health check with all mocked dependencies" in {
    val mockConfigService = mock(classOf[ConfigurationServiceInterface])
    val mockDbProvider = mock(classOf[DatabaseConfigProvider])
    val mockConnection = mock(classOf[Connection])
    val mockStatement = mock(classOf[PreparedStatement])
    val mockResultSet = mock(classOf[ResultSet])
    
    // Configuration des sections de config
    val mockDbConfig = mock(classOf[DatabaseConfigSection])
    when(mockConfigService.get[DatabaseConfigSection](any())).thenReturn(mockDbConfig)
    
    val mockDataMgrConfig = mock(classOf[DataManagerConfigSection])
    when(mockConfigService.get[DataManagerConfigSection](any())).thenReturn(mockDataMgrConfig)
    
    // Simuler une connexion saine
    when(mockDbProvider.getConnection()).thenReturn(mockConnection)
    when(mockConnection.prepareStatement("SELECT 1")).thenReturn(mockStatement)
    when(mockStatement.executeQuery()).thenReturn(mockResultSet)
    when(mockResultSet.next()).thenReturn(true)
    
    val dataManager = new DataManager(mockConfigService, mockDbProvider)
    
    // Test de santé
    val isHealthy = dataManager.healthCheck()
    isHealthy shouldBe true
  }
}

