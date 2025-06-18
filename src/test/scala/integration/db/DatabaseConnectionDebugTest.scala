package integration.db

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatabaseConnectionDebugTest extends AnyFlatSpec with Matchers {
  "Database connection parameters" should "be properly configured" in {
    // Print all relevant environment variables
    println("=== DATABASE CONNECTION DEBUG INFO ===")
    println(s"DATABASE_HOST: ${sys.env.getOrElse("DATABASE_HOST", "not set")}")
    println(s"DATABASE_PORT: ${sys.env.getOrElse("DATABASE_PORT", "not set")}")
    println(s"DATABASE_NAME: ${sys.env.getOrElse("DATABASE_NAME", "not set")}")
    println(s"DATABASE_USER: ${sys.env.getOrElse("DATABASE_USER", "not set")}")
    println(s"DATABASE_URL: ${sys.env.getOrElse("DATABASE_URL", "not set")}")
    println(s"TEST_MODE: ${sys.env.getOrElse("TEST_MODE", "not set")}")
    println(s"DOCKER_CONTAINER: ${sys.env.getOrElse("DOCKER_CONTAINER", "not set")}")
    println(s"test.mode property: ${sys.props.get("test.mode")}")
    println(s"test.database.enabled property: ${sys.props.get("test.database.enabled")}")
    
    // Try to establish a connection
    try {
      val dbHost = sys.env.getOrElse("DATABASE_HOST", "test-db")
      val dbPort = sys.env.getOrElse("DATABASE_PORT", "5432")
      val dbName = sys.env.getOrElse("DATABASE_NAME", "test_recommendation_engine")
      val jdbcUrl = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
      val username = sys.env.getOrElse("DATABASE_USER", "postgres")
      val password = sys.env.getOrElse("DATABASE_PASSWORD", "test_password")
      
      println(s"Attempting connection to: $jdbcUrl")
      val conn = java.sql.DriverManager.getConnection(jdbcUrl, username, password)
      println("Connection established successfully!")
      conn.close()
    } catch {
      case e: Exception =>
        println(s"Connection failed: ${e.getMessage}")
        e.printStackTrace()
    }
    
    succeed
  }
}

