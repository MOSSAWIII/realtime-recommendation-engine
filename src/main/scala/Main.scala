import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Main {
  def main(args: Array[String]): Unit = {
    println("Event Recommendation Engine")
    println("==========================")
    
    val userId = "user123" // Exemple d'utilisateur
    
    // Simulation d'interactions utilisateur
    UserInteractionService.updateSearchQuery(userId, "concert musique")
    UserInteractionService.updateLocation(userId, "Paris, France")
    UserInteractionService.updateTimeRange(
      userId, 
      LocalDateTime.now(), 
      LocalDateTime.now().plusMonths(1)
    )
    
    // Obtenir des recommandations
    val recommendations = RecommendationEngine.getRecommendations(userId)
    
    println(s"Recommandations pour l'utilisateur $userId:")
    recommendations.foreach { event =>
      println(s"- ${event.title} (${event.city}, ${event.startTime.format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))})")
      println(s"  ${event.description.getOrElse("")}")
      println(s"  ${event.attendeeCount} participants")
      println()
    }
  }
  
  // Méthode de démonstration pour simuler un clic de l'utilisateur
  def simulateUserClick(userId: String, eventId: String): Unit = {
    UserInteractionService.addClickedEvent(userId, eventId)
    println(s"L'utilisateur $userId a cliqué sur l'événement $eventId")
    
    // Mettre à jour les recommandations après un clic
    val updatedRecommendations = RecommendationEngine.getRecommendations(userId)
    println("Recommandations mises à jour:")
    updatedRecommendations.foreach(event => println(s"- ${event.title}"))
  }
/**
 * Ce fichier (Main.scala) sert de point d'entrée pour le système de recommandation en temps réel.
 * Il initialise l'application, configure les composants nécessaires et lance le processus de génération
 * de recommandations en temps réel pour améliorer l'expérience utilisateur.
 */
}
