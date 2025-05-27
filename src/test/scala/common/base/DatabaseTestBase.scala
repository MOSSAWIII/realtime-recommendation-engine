package common.base

import java.sql.Connection
import scala.concurrent.Await
import scala.concurrent.duration._

import infrastructure.config.{ConfigurationServiceInterface, DatabaseConfigProvider}
import common.utils.TestResourceManager

/**
 * Base de test pour les tests utilisant une base de données
 */
trait DatabaseTestBase extends IsolatedTestBase {
  
  private var testConnections = scala.collection.mutable.ListBuffer.empty[Connection]
  
  // Ajout de la référence au dbProvider
  protected def dbProvider: DatabaseConfigProvider
  
  /**
   * Crée une connexion isolée pour ce test
   */
  protected def createTestConnection(dbProvider: DatabaseConfigProvider): Connection = {
    val conn = dbProvider.getConnection()
    testConnections += conn
    conn
  }
  
  /**
   * Nettoie les tables après le test
   */
  protected def cleanupTestTables(dataManager: application.core.DataManager): Unit = {
    // Liste des tables à nettoyer
    val tables = List(
      "processed_events", 
      "user_events", 
      "event_participants", 
      "events", 
      "user_friends", 
      "users"
    )
    
    // Utilisez le dbProvider du trait
    val conn = createTestConnection(dbProvider)
    try {
      val stmt = conn.createStatement()
      tables.foreach { table =>
        try {
          stmt.execute(s"TRUNCATE TABLE $table CASCADE")
        } catch {
          case e: Exception =>
            println(s"Error truncating table $table: ${e.getMessage}")
        }
      }
      stmt.close()
    } finally {
      conn.close()
    }
  }
  
  override def afterEach(): Unit = {
    // Fermer toutes les connexions créées
    testConnections.foreach { conn =>
      try {
        if (!conn.isClosed) {
          conn.close()
        }
      } catch {
        case e: Exception => 
          println(s"Error closing connection: ${e.getMessage}")
      }
    }
    testConnections.clear()
    
    super.afterEach()
  }
}

