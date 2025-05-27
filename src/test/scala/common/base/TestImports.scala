package common.base

// Centralisation des imports pour tous les tests
object TestImports {
  // Mockito
  val mockitoImports = Seq(
    "org.mockito.Mockito._",
    "org.mockito.ArgumentMatchers._",
    "org.scalatestplus.mockito.MockitoSugar"
  )
  
  // Autres imports fréquemment utilisés
  val commonImports = Seq(
    "scala.concurrent.{Future, ExecutionContext}",
    "scala.concurrent.duration._"
  )
}

