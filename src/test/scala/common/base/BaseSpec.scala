package common.base

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.ExecutionContext

trait BaseSpec {
  implicit val ec: ExecutionContext = ExecutionContext.global
}

trait UnitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BaseSpec {
  // Common unit test setup
}

trait IntegrationSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with BaseSpec {
  // Common integration test setup
}

trait FunctionalSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BaseSpec {
  // Common functional test setup
}

