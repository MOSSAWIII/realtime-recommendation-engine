package domain.models

case class HistoricalDataImport(
  dataType: String,
  records: List[Map[String, Any]]
)
