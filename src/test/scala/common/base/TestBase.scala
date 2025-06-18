package common.base

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.flatspec.AnyFlatSpec
import scala.concurrent.ExecutionContext

/**
 * Base trait for all test classes providing common functionality.
 * All test classes should extend one of the specialized traits below.
 */
trait TestBase {
  implicit val ec: ExecutionContext = ExecutionContext.global
}

/**
 * Base trait for unit tests using FlatSpec style.
 * Use for isolated component tests that don't require external dependencies.
 * 
 * Example usage:
 * {{{
 * class MyComponentSpec extends UnitTestBase {
 *   "My component" should "do something" in {
 *     // test code
 *   }
 * }
 * }}}
 */
trait UnitTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TestBase

/**
 * Base trait for unit tests using WordSpec style
 */
trait UnitWordSpecBase extends AnyWordSpec with Matchers with BeforeAndAfterEach with TestBase

/**
 * Base trait for integration tests
 */
trait IntegrationTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestBase {
  // Pas de redéfinition d'ExecutionContext ici
}

/**
 * Base trait for functional tests
 */
trait FunctionalTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterAll with TestBase

