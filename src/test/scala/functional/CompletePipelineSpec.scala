package functional

import common.base.MessagingTestBase
import application.core.{Transformer, Recommender}
import domain.models._
import scala.concurrent.{Promise, ExecutionContext}
import scala.concurrent.duration._
import org.scalatest.concurrent.ScalaFutures._
import common.utils.TestResourceManager

class CompletePipelineSpec extends MessagingTestBase {
  // Pas besoin de redéfinir ec, il est hérité de TestBase via MessagingTestBase
  
  "Complete pipeline" should "process events end-to-end" in {
    // Setup
    val mockDataManager = createIsolatedDataManager()
    val recommendationPromise = Promise[Recommendation]()
    
    // Setup channels
    recommendationChannel = createTestChannel("recommendation-channel")
    
    // Create isolated queue
    val queueName = createIsolatedQueue(recommendationChannel, "recommendation")
    
    // Create components with ExecutionContext
    val transformer = new Transformer(mockDataManager)(ec)
    registerForCleanup(new AutoCloseable {
      override def close(): Unit = transformer.close()
    })
    
    val recommender = new Recommender(mockDataManager)(ec)
    registerForCleanup(new AutoCloseable {
      override def close(): Unit = recommender.close()
    })
    
    // Create test event
    val testUser = UserEvent(
      userId = TestResourceManager.uniqueId(),
      eventType = "view",
      itemId = Some(TestResourceManager.uniqueId()),
      timestamp = System.currentTimeMillis()
    )
    
    // Process pipeline
    val processedEvent = transformer.processEvent(testUser)
    
    whenReady(processedEvent, timeout(15.seconds)) { processed =>
      val recommendationRequest = RecommendationRequest(processed.userId)
      val recommendation = recommender.generateRecommendations(recommendationRequest)
      
      whenReady(recommendation, timeout(15.seconds)) { rec =>
        rec should not be null
        rec.userId shouldBe testUser.userId
      }
    }
  }
}