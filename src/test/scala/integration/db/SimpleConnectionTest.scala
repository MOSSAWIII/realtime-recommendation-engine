// filepath: src/test/scala/integration/db/SimpleConnectionTest.scala
package integration.db

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.{Connection, DriverManager}

class SimpleConnectionTest extends AnyFlatSpec with Matchers {
  "Database connection" should "be established successfully" in {
    val dbHost = sys.env.getOrElse("DATABASE_HOST", "test-db")
    val dbPort = sys.env.getOrElse("DATABASE_PORT", "5432")
    val dbName = sys.env.getOrElse("DATABASE_NAME", "test_recommendation_engine")
    val jdbcUrl = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
    val username = sys.env.getOrElse("DATABASE_USER", "postgres")
    val password = sys.env.getOrElse("DATABASE_PASSWORD", "test_password")
    
    println(s"Connecting to database: $jdbcUrl with user: $username")
    
    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(jdbcUrl, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT 1")
      resultSet.next() shouldBe true
      resultSet.getInt(1) shouldBe 1
      
      println("Connection and query successful!")
      succeed
    } finally {
      if (connection != null) connection.close()
    }
  }
}

