package models

case class Event(
  id: Option[Long],  // Modifier de eventId à id pour correspondre aux tests
  name: String,
  description: Option[String] = None,
  startTime: Long,
  endTime: Option[Long] = None,
  location: Option[String] = None,
  createdBy: Long,  // Modifier de organizerId à createdBy
  status: String = "active",  // Ajouter ce champ manquant
  keywords: List[String] = List.empty,
  keywordFrequencies: Map[String, Int] = Map.empty
)