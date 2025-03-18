import scala.collection.mutable

object UserInteractionService {
  private val userInteractions = mutable.Map[Int, UserInteraction]()
  
  def getUserInteraction(userId: Int): Option[UserInteraction] = {
    userInteractions.get(userId)
  }
  
  def updateSearchQuery(userId: Int, query: String): Unit = {
    val interaction = userInteractions.getOrElse(userId, 
      UserInteraction(userId, None, List.empty, None, None))
    userInteractions(userId) = interaction.copy(searchQuery = Some(query))
  }
  
  def addClickedEvent(userId: String, eventId: String): Unit = {
    val interaction = userInteractions.getOrElse(userId, 
      UserInteraction(userId, None, List.empty, None, None))
    val updatedClicks = (eventId :: interaction.clickedEventIds).distinct.take(10) // gardons un historique limité
    userInteractions(userId) = interaction.copy(clickedEventIds = updatedClicks)
  }
  
  def updateLocation(userId: String, location: String): Unit = {
    val interaction = userInteractions.getOrElse(userId, 
      UserInteraction(userId, None, List.empty, None, None))
    userInteractions(userId) = interaction.copy(currentLocation = Some(location))
  }
  
  def updateTimeRange(userId: String, start: LocalDateTime, end: LocalDateTime): Unit = {
    val interaction = userInteractions.getOrElse(userId, 
      UserInteraction(userId, None, List.empty, None, None))
    userInteractions(userId) = interaction.copy(timeRange = Some((start, end)))
  }
}