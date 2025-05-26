package common.base

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.flatspec.AnyFlatSpec
import scala.concurrent.ExecutionContext

/**
 * Base trait for all test classes
 */
trait TestBase {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
}

/**
 * Base trait for unit tests using FlatSpec style
 */
trait UnitTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TestBase

/**
 * Base trait for unit tests using WordSpec style
 */
trait UnitWordSpecBase extends AnyWordSpec with Matchers with BeforeAndAfterEach with TestBase

/**
 * Base trait for integration tests
 */
trait IntegrationTestBase extends AnyWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestBase {
  // Common setup/teardown for integration tests
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Common integration test setup
  }
  
  override def afterAll(): Unit = {
    // Common integration test cleanup
    super.afterAll()
  }
}

/**
 * Base trait for functional tests
 */
trait FunctionalTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterAll with TestBase

