import java.time.LocalDateTime

object RecommendationEngine {
  
  def getRecommendations(userId: Int, count: Int = 5): List[Event] = {
    val events = DataLoader.loadEvents()
    val users = DataLoader.loadUsers()
    val userOpt = users.find(_.userId == userId)
    val userInteractionOpt = UserInteractionService.getUserInteraction(userId)
    
    if (userInteractionOpt.isEmpty) {
      // Pour les nouveaux utilisateurs, recommandations basées sur la localisation et la popularité
      val userLocation = userOpt.flatMap(_.location).map(extractCity)
      events
        .filter(event => userLocation.forall(loc => 
          event.city.exists(_.equalsIgnoreCase(loc))))
        .sortBy(event => {
          val keywordSum = event.keywordFrequencies.values.sum // Utilisation de la somme des fréquences comme indicateur de popularité
          -keywordSum // Tri décroissant
        })
        .take(count)
    } else {
      // Pour les utilisateurs avec des interactions
      val interaction = userInteractionOpt.get
      
      // Calcul des scores pour chaque événement
      val eventScores = events.map { event =>
        val contentScore = calculateContentScore(event, interaction)
        val proximityScore = calculateProximityScore(event, interaction)
        val timeScore = calculateTimeScore(event, interaction)
        val popularityScore = calculatePopularityScore(event)
        
        // Score total pondéré
        val totalScore = (contentScore * 0.4) + 
                        (proximityScore * 0.3) + 
                        (timeScore * 0.2) + 
                        (popularityScore * 0.1)
                        
        (event, totalScore)
      }
      
      // Trier par score et prendre les N premiers
      eventScores.sortBy(-_._2).map(_._1).take(count)
    }
  }
  
  // Extraction de la ville à partir d'une chaîne de localisation
  private def extractCity(location: String): String = {
    location.split(",").headOption.getOrElse("").trim
  }
  
  // Calculer le score basé sur le contenu (fréquences de mots-clés vs recherche)
  private def calculateContentScore(event: Event, interaction: UserInteraction): Double = {
    interaction.searchQuery match {
      case None => 0.0
      case Some(query) =>
        // Convertir la requête en ensemble de mots
        val queryWords = query.toLowerCase.split("\\s+").toSet
        
        // Calculer le score basé sur les fréquences des mots-clés
        val matchingKeywords = event.keywordFrequencies.filter { case (keywordId, freq) =>
          // Ici, vous devriez avoir une map de correspondance entre keywordId et mot réel
          // Pour l'instant, on utilise juste la présence de fréquence comme indicateur
          freq > 0
        }
        
        val totalFrequency = matchingKeywords.values.sum.toDouble
        val normalizedScore = if (totalFrequency > 0) 
          math.min(1.0, totalFrequency / 100.0) else 0.0
        
        normalizedScore
    }
  }
  
  // Calculer le score de proximité géographique
  private def calculateProximityScore(event: Event, interaction: UserInteraction): Double = {
    interaction.currentLocation match {
      case None => 0.0
      case Some(location) =>
        val city = extractCity(location)
        if (event.city.exists(_.equalsIgnoreCase(city))) 1.0 else 0.0
    }
  }
  
  // Calculer le score selon la période temporelle
  private def calculateTimeScore(event: Event, interaction: UserInteraction): Double = {
    interaction.timeRange match {
      case None => 0.5  // Score neutre si aucune préférence de temps
      case Some((start, end)) =>
        if (event.startTime.isAfter(start) && event.startTime.isBefore(end)) 1.0
        else 0.0
    }
  }
  
  // Calculer le score de popularité basé sur les fréquences de mots-clés
  private def calculatePopularityScore(event: Event): Double = {
    val totalFrequency = event.keywordFrequencies.values.sum.toDouble
    math.min(1.0, totalFrequency / 1000.0) // Normalisation avec un maximum de 1.0
  }
}
