package application.core

import java.sql.{Connection, SQLException}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, TimeUnit, Executors}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory
import utils.metrics.MetricsRegistry
import utils.error.DatabaseException

/**
 * Transaction management for DataManager operations.
 */
class DataManagerTransactions(dataManager: DataManager)(implicit ec: ExecutionContext) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val componentName = "data-manager-transactions"
  
  // Metrics
  private val transactionCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "transactions", "count"),
    "Number of transactions executed",
    "transactions"
  )
  
  private val transactionSuccessCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "transactions", "success"),
    "Number of successful transactions",
    "transactions"
  )
  
  private val transactionFailureCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "transactions", "failure"),
    "Number of failed transactions",
    "errors"
  )
  
  private val transactionRollbackCounter = MetricsRegistry.buildCounter(
    MetricsRegistry.MetricMetadata(componentName, "transactions", "rollback"),
    "Number of rolled back transactions",
    "rollbacks"
  )
  
  private val transactionDuration = MetricsRegistry.buildHistogram(
    MetricsRegistry.MetricMetadata(componentName, "transactions", "duration"),
    "Transaction duration distribution",
    "milliseconds"
  )
  
  // Active transactions tracking
  private val activeTransactions = new ConcurrentHashMap[Long, TransactionInfo]()
  private val transactionIdCounter = new AtomicLong(0)
  
  // Transaction tracking info
  private case class TransactionInfo(
    id: Long,
    startTime: Long,
    threadName: String,
    stackTrace: Array[StackTraceElement]
  )
  
  // Create a dedicated scheduler
  private val scheduler = Executors.newSingleThreadScheduledExecutor()

  /**
   * Start monitoring for transactions.
   */
  def startMonitoring(): Unit = {
    logger.info("Starting transaction monitoring")
    
    // Schedule a task to log long-running transactions
    scheduler.scheduleAtFixedRate(
      () => monitorLongRunningTransactions(),
      30,
      30,
      TimeUnit.SECONDS
    )
  }
  
  /**
   * Monitor for long-running transactions.
   */
  private def monitorLongRunningTransactions(): Unit = {
    val longRunningThreshold = 60000L // 60 seconds
    val now = System.currentTimeMillis()
    
    import scala.jdk.CollectionConverters._
    activeTransactions.entrySet().asScala.foreach { entry =>
      val txInfo = entry.getValue
      val duration = now - txInfo.startTime
      
      if (duration > longRunningThreshold) {
        logger.warn(s"Long-running transaction detected: ID=${txInfo.id}, duration=${duration}ms, thread=${txInfo.threadName}")
        // Log stack trace at debug level
        if (logger.isDebugEnabled) {
          val stackTrace = txInfo.stackTrace.map(frame => s"\t${frame}").mkString("\n")
          logger.debug(s"Transaction ${txInfo.id} stack trace:\n$stackTrace")
        }
      }
    }
  }
  
  /**
   * Execute a database operation with a transaction.
   *
   * @param operation Function that takes a connection and returns a result
   * @return Future containing the result of the operation
   */
  def withTransaction[T](operation: Connection => T): Future[T] = {
    val conn = dataManager.getConnection()
    val transactionId = transactionIdCounter.incrementAndGet()
    transactionCounter.inc()
    
    logger.debug(s"Starting transaction $transactionId")
    val startTime = System.currentTimeMillis()
    
    val txInfo = TransactionInfo(
      transactionId,
      startTime,
      Thread.currentThread().getName,
      Thread.currentThread().getStackTrace
    )
    activeTransactions.put(transactionId, txInfo)
    
    Future {
      // Déclarer originalAutoCommit en dehors du bloc try pour qu'il soit visible dans finally
      var originalAutoCommit = true
      try {
        // Capturer la valeur initiale avant de la modifier
        originalAutoCommit = conn.getAutoCommit
        conn.setAutoCommit(false)
        
        val result = operation(conn)
        
        conn.commit()
        val endTime = System.currentTimeMillis()
        transactionSuccessCounter.inc()
        
        logger.debug(s"Transaction $transactionId committed successfully in ${endTime - startTime}ms")
        activeTransactions.remove(transactionId)
        
        result
      } catch {
        case e: Exception =>
          try {
            conn.rollback()
            transactionRollbackCounter.inc()
          } catch {
            case rollbackEx: SQLException =>
              logger.error(s"Failed to rollback transaction $transactionId", rollbackEx)
          }
          
          transactionFailureCounter.inc()
          activeTransactions.remove(transactionId)
          
          logger.error(s"Transaction $transactionId failed: ${e.getMessage}", e)
          throw DatabaseException(s"Transaction failed: ${e.getMessage}", e, Map(
            "transactionId" -> transactionId.toString,
            "threadName" -> Thread.currentThread().getName,
            "duration" -> (System.currentTimeMillis() - startTime).toString
          ))
      } finally {
        try {
          // Ensure we always set autoCommit back to its original value
          if (!conn.isClosed) {
            conn.setAutoCommit(originalAutoCommit)
            conn.close()
          }
          logger.debug(s"Connection closed for transaction $transactionId")
        } catch {
          case e: SQLException =>
            logger.warn(s"Failed to close connection for transaction $transactionId: ${e.getMessage}")
        }
        // Ensure the transaction is removed from activeTransactions
        activeTransactions.remove(transactionId)
      }
    }
  }
  
  /**
   * Close transaction monitoring resources.
   */
  def close(): Unit = {
    logger.info("Shutting down transaction manager")
    
    // Log information about any active transactions
    import scala.jdk.CollectionConverters._
    val activeCount = activeTransactions.size()
    if (activeCount > 0) {
      logger.warn(s"Shutting down with $activeCount active transactions")
      
      activeTransactions.entrySet().asScala.foreach { entry =>
        val txInfo = entry.getValue
        val duration = System.currentTimeMillis() - txInfo.startTime
        logger.warn(s"Active transaction during shutdown: ID=${txInfo.id}, duration=${duration}ms, thread=${txInfo.threadName}")
      }
    }
  }
}
