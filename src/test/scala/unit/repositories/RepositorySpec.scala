package unit.repositories

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import repositories.Repository
import models.{Recommendation, RecommendedItem}
import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

class RepositorySpec extends AnyFlatSpec with Matchers with MockitoSugar {
  
  "Repository" should "store and retrieve recommendations" in {
    // Create mocks
    val mockConnection = mock[Connection]
    val mockPreparedStatement = mock[PreparedStatement] 
    val mockResultSet = mock[ResultSet]
    
    // Configure mock behavior
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement)
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet)
    when(mockResultSet.next()).thenReturn(true, false)
    
    when(mockResultSet.getLong("id")).thenReturn(1L)
    when(mockResultSet.getLong("user_id")).thenReturn(42L)
    when(mockResultSet.getString("recommended_items")).thenReturn("[{\"id\":101,\"score\":0.9,\"reason\":\"test\"}]")
    when(mockResultSet.getDouble("score")).thenReturn(0.95)
    when(mockResultSet.getLong("timestamp")).thenReturn(System.currentTimeMillis())
    
    // Create test repository with mock connection
    val repository = new TestableRepository(mockConnection)
    
    // Create test recommendation
    val recommendation = Recommendation(
      id = Some(1),
      userId = 42,
      recommendedItems = List(RecommendedItem(101, 0.9, "test")),
      score = 0.95,
      timestamp = System.currentTimeMillis()
    )
    
    // Test storing recommendation
    val storedFuture = repository.storeRecommendation(recommendation)
    val stored = Await.result(storedFuture, 5.seconds)
    
    stored.id shouldBe defined
    stored.userId shouldBe 42
    
    // Verify the prepared statement was called with correct parameters
    verify(mockPreparedStatement).setLong(1, 42)
    
    // Test retrieving recommendations 
    val retrievedFuture = repository.getRecommendationForUser(42)
    val retrieved = Await.result(retrievedFuture, 5.seconds)
    
    retrieved should not be empty
    retrieved.get.userId shouldBe 42
  }
}