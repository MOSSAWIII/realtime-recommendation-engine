package unit.common.mocks

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import common.mocks.{MockFactory, TestConfigurationService, MockDataManager}

class MockFactorySpec extends AnyFlatSpec with Matchers {
  "MockFactory" should "create consistent TestConfigurationService instances" in {
    val config1 = MockFactory.createConfigService()
    val config2 = MockFactory.createConfigService()
    
    config1.getClass shouldBe config2.getClass
    config1 should not be theSameInstanceAs(config2)
  }

  it should "create configured DataManager" in {
    val dataManager = MockFactory.createDataManager()
    
    dataManager shouldBe a[MockDataManager]
    // Ou tester le comportement au lieu des propriétés internes
    dataManager should not be null
  }
}

