import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExampleSpec extends AnyFlatSpec with Matchers {
  "A simple test" should "verify that 1 + 1 equals 2" in {
    assert(1 + 1 == 2)
  }

  it should "verify that 2 * 2 equals 4" in {
    assert(2 * 2 == 4)
  }
}