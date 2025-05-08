package models

case class Event(
  id: Option[Long],  // The Recommender uses id directly, not Option[Long]
  name: String,
  description: Option[String] = None,
  startTime: Long,
  endTime: Option[Long] = None,
  location: Option[String] = None,
  createdBy: Long,
  status: String = "active",
  keywords: List[String] = List.empty,
  keywordFrequencies: Map[String, Int] = Map.empty,
  participants: Set[Long] = Set.empty
)