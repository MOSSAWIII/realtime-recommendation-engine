package common.fixtures

import java.time.Instant
import java.util.UUID

/**
 * Données partagées pour les tests
 */
object TestData {
  // Générateurs d'ID uniques pour les tests
  def uniqueId(): Long = System.currentTimeMillis()
  def uniqueString(): String = UUID.randomUUID().toString
  
  // Autres méthodes de génération de données de test selon les besoins
}
