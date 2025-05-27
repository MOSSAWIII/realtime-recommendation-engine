// filepath: src/test/scala/common/utils/TestLogger.scala
package common.utils

import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.MDC
import scala.util.Try

/**
 * Utilitaire pour la gestion des logs dans les tests
 */
object TestLogger {
  
  /**
   * Configure les logs pour un test spécifique
   * @param testClass Classe de test à laquelle s'appliquent les logs
   * @param testId Identifiant unique du test
   */
  def configureTestLogging(testClass: Class[_], testId: String): Logger = {
    val logger = LoggerFactory.getLogger(testClass)
    MDC.put("testRunId", testId)
    MDC.put("testClass", testClass.getSimpleName)
    
    // Configure le chemin du fichier de log pour ce test
    val testLogFile = s"target/test-logs/${testClass.getSimpleName}-${testId}.log"
    System.setProperty("test.log.file", testLogFile)
    
    // Ajuste le niveau de log selon les propriétés système
    if (System.getProperty("test.debug") == "true") {
      System.setProperty("log.level.unit", "DEBUG")
      System.setProperty("log.level.root", "DEBUG")
    }
    
    logger.info(s"Test logging configured for ${testClass.getSimpleName} with ID $testId")
    
    logger
  }
  
  /**
   * Nettoie les ressources de logging à la fin d'un test
   */
  def cleanupLogging(): Unit = {
    Try(MDC.clear())
  }
}

