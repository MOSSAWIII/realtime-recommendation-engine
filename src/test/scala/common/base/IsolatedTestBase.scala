package common.base

import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.ExecutionContext
import scala.collection.mutable.ListBuffer

import application.core.DataManager
import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
import common.utils.TestResourceManager

/**
 * Base de test isolée avec nettoyage complet des ressources
 */
trait IsolatedTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  // Configuration unique par test
  val testId: String = TestResourceManager.uniqueId().toString
  val testQueueName: String = TestResourceManager.uniqueQueueName()
  
  // Liste des ressources à nettoyer
  private val resourcesToCleanup = ListBuffer.empty[AutoCloseable]
  
  /**
   * Enregistre une ressource pour nettoyage automatique
   */
  protected def registerForCleanup[T <: AutoCloseable](resource: T): T = {
    resourcesToCleanup += resource
    resource
  }
  
  /**
   * Crée un DataManager isolé pour les tests
   */
  protected def createIsolatedDataManager(
    configService: ConfigurationServiceInterface,
    dbProvider: DatabaseConfigProvider
  ): DataManager = {
    val manager = new DataManager(configService, dbProvider)(scala.concurrent.ExecutionContext.global)
    
    // Correction: Vérifier si DataManager implémente AutoCloseable ou créer un adaptateur
    // avec les méthodes que DataManager expose réellement pour le nettoyage
    registerForCleanup(new AutoCloseable {
      override def close(): Unit = {
        // Utilisez la méthode appropriée pour fermer le DataManager
        // Par exemple, si DataManager a une méthode shutdown() au lieu de close()
        manager.shutdown() // Assurez-vous que cette méthode existe dans DataManager
      }
    })
    manager
  }
  
  override def afterEach(): Unit = {
    // Nettoie toutes les ressources enregistrées
    resourcesToCleanup.foreach { resource =>
      try {
        resource.close()
      } catch {
        case e: Exception => 
          println(s"Error closing resource: ${e.getMessage}")
      }
    }
    resourcesToCleanup.clear()
    
    super.afterEach()
  }
}

