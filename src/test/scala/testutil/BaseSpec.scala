package testutil

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

// Base spec for unit tests using FlatSpec style
abstract class UnitSpec extends AnyFlatSpec with Matchers {
  implicit val ec: ExecutionContext = global
}

// Base spec for unit tests using FunSuite style
abstract class UnitSuiteSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {
  implicit val ec: ExecutionContext = global
}

// Base spec for integration tests
abstract class IntegrationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  implicit val ec: ExecutionContext = global
}

// Base spec for repository tests
abstract class RepositorySpec extends UnitSpec with BeforeAndAfterEach {
  // Common repository test setup 
  val mockConnection = MockProvider.mockDatabaseConfig()
}

