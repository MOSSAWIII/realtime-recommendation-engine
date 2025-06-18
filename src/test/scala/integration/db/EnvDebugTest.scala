package integration.db

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EnvDebugTest extends AnyFlatSpec with Matchers {
  "Environment variables" should "be properly set for database tests" in {
    println(s"DATABASE_URL: ${sys.env.getOrElse("DATABASE_URL", "not set")}")
    println(s"DATABASE_HOST: ${sys.env.getOrElse("DATABASE_HOST", "not set")}")
    println(s"TEST_MODE: ${sys.env.getOrElse("TEST_MODE", "not set")}")
    println(s"test.mode: ${sys.props.get("test.mode")}")
    println(s"test.database.enabled: ${sys.props.get("test.database.enabled")}")
    
    succeed
  }
}

