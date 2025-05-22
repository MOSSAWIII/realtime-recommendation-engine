package infrastructure.persistence

import domain.models.{UserInteraction, ProcessedEvent}
import scala.concurrent.{ExecutionContext, Future}

class UserInteractionRepository(implicit override val ec: ExecutionContext) extends Repository {
  def saveInteraction(interaction: UserInteraction): Future[Int] = {
    // Implementation
    Future.successful(1)
  }
  
  def getRecentInteractions(userId: Long, limit: Int): Future[Seq[UserInteraction]] = {
    // Implementation
    Future.successful(Seq.empty)
  }
  
  // Convert to ProcessedEvent for compatibility with some components
  def convertToProcessedEvent(interaction: UserInteraction): ProcessedEvent = {
    ProcessedEvent(
      id = interaction.id,
      userId = interaction.userId,
      eventType = interaction.interactionType,
      itemId = Some(interaction.itemId), // Utiliser Some() pour le convertir en Option[Long]
      timestamp = interaction.timestamp
    )
  }
}