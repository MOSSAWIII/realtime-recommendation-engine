// filepath: src/test/scala/common/base/MockitoSpec.scala
package common.base

import org.mockito.Mockito
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

trait MockitoSpec extends AnyWordSpec with Matchers {
  // Méthode auxiliaire pour créer des mocks sans ambiguïté
  def mock[T](implicit manifest: Manifest[T]): T = {
    Mockito.mock(manifest.runtimeClass.asInstanceOf[Class[T]])
  }
}

