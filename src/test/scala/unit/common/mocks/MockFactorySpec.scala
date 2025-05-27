package unit.common.mocks

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import common.mocks.MockFactory

class MockFactorySpec extends AnyFlatSpec with Matchers {
  
  "MockFactory" should "create consistent TestConfigurationService instances" in {
    val config1 = MockFactory.createConfigService()
    val config2 = MockFactory.createConfigService()
    
    config1.getClass shouldBe config2.getClass
    config1 should not be theSameInstanceAs(config2)
  }
  
  it should "create MockDataManager with appropriate dependencies" in {
    val dataManager = MockFactory.createDataManager()
    
    dataManager.configService shouldBe a[common.mocks.TestConfigurationService]
    dataManager.dbProvider shouldBe a[common.mocks.TestDatabaseProvider]
  }
}

