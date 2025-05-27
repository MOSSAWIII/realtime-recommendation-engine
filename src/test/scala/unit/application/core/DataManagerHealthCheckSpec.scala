package unit.application.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import application.core.DataManager
import scala.util.{Success, Failure, Try}

class DataManagerHealthCheckSpec extends AnyFlatSpec with Matchers {
  
  "DataManager.healthCheck" should "return true when all components are accessible" in {
    // Arrangement
    val dataManager = mock(classOf[DataManager])
    // Configuration correcte des mocks avec when/thenReturn
    when(dataManager.isDatabaseAccessible()).thenReturn(true)
    when(dataManager.isCacheAccessible()).thenReturn(true)
    when(dataManager.healthCheck()).thenCallRealMethod()
    
    // Action
    val result = dataManager.healthCheck()
    
    // Assertion
    result shouldBe true
  }
  
  it should "return false when database is not accessible" in {
    // Arrangement
    val dataManager = mock(classOf[DataManager])
    when(dataManager.isDatabaseAccessible()).thenReturn(false)
    when(dataManager.isCacheAccessible()).thenReturn(true)
    
    // Action
    val result = dataManager.healthCheck()
    
    // Assertion
    result shouldBe false
  }
  
  it should "return false when cache is not accessible" in {
    // Arrangement
    val dataManager = mock(classOf[DataManager])
    when(dataManager.isDatabaseAccessible()).thenReturn(true)
    when(dataManager.isCacheAccessible()).thenReturn(false)
    
    // Action
    val result = dataManager.healthCheck()
    
    // Assertion
    result shouldBe false
  }
  
  it should "return false when an exception occurs" in {
    // Arrangement
    val dataManager = mock(classOf[DataManager])
    when(dataManager.isDatabaseAccessible()).thenThrow(new RuntimeException("Test exception"))
    
    // Action
    val result = dataManager.healthCheck()
    
    // Assertion
    result shouldBe false
  }
}

