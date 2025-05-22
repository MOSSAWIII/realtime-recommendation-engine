package common.utils

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.flatspec.AnyFlatSpec
import scala.concurrent.ExecutionContext

// Trait de base pour tous les tests
trait TestBase {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
}

// Pour tests unitaires
trait UnitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with TestBase

// Pour tests d'intégration
trait IntegrationSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestBase {
  // Setup/teardown commun
}

// Pour tests fonctionnels
trait FunctionalSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with TestBase
