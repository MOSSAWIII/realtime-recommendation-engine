package application.core

import java.sql.{Connection, SQLException}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
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
  private val transactionCounter = MetricsRegistry.counter(s"$componentName.transactions.count")
  private val transactionSuccessCounter = MetricsRegistry.counter(s"$componentName.transactions.success")
  private val transactionFailureCounter = MetricsRegistry.counter(s"$componentName.transactions.failure")
  private val transactionRollbackCounter = MetricsRegistry.counter(s"$componentName.transactions.rollback")
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
  
  /**
   * Start monitoring for transactions.
   */
  def startMonitoring(): Unit = {
    logger.info("Starting transaction monitoring")
    
    // Schedule a task to log long-running transactions
    MetricsRegistry.scheduler.scheduleAtFixedRate(
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
    transactionCounter.inc()
    val txId = transactionIdCounter.incrementAndGet()
    val startTime = System.currentTimeMillis()
    
    // Create transaction info for monitoring
    val txInfo = TransactionInfo(
      id = txId,
      startTime = startTime,
      threadName = Thread.currentThread().getName,
      stackTrace = Thread.currentThread().getStackTrace
    )
    
    activeTransactions.put(txId, txInfo)
    
    // Get connection from pool
    var conn: Connection = null
    try {
      conn = dataManager.getConnection()
      
      // MySQL and PostgreSQL default to auto-commit
      conn.setAutoCommit(false)
      
      // Execute the operation
      Future {
        try {
          val result = operation(conn)
          conn.commit()
          transactionSuccessCounter.inc()
          result
        } catch {
          case e: Exception =>
            try {
              if (conn != null && !conn.isClosed) {
                conn.rollback()
                transactionRollbackCounter.inc()
                logger.warn(s"Transaction rolled back due to: ${e.getMessage}")
              }
            } catch {
              case re: SQLException =>
                logger.error(s"Failed to rollback transaction: ${re.getMessage}", re)
            }
            transactionFailureCounter.inc()
            throw new DatabaseException(s"Transaction failed: ${e.getMessage}", e)
        } finally {
          val duration = System.currentTimeMillis() - startTime
          transactionDuration.update(duration)
          activeTransactions.remove(txId)
          
          // Close connection
          if (conn != null) {
            try {
              conn.setAutoCommit(true) // Reset to default state
              conn.close()
            } catch {
              case e: SQLException =>
                logger.warn(s"Failed to close connection: ${e.getMessage}", e)
            }
          }
        }
      }
    } catch {
      case e: Exception =>
        // Handle connection acquisition failure
        activeTransactions.remove(txId)
        transactionFailureCounter.inc()
        Future.failed(new DatabaseException(s"Failed to acquire database connection: ${e.getMessage}", e))
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
