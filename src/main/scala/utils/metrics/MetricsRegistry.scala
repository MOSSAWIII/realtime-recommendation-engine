package utils.metrics

import java.util.concurrent.{ConcurrentHashMap, TimeUnit, Executors, ScheduledExecutorService}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.slf4j.LoggerFactory
import infrastructure.config.ConfigurationService
import infrastructure.config.MetricsConfigSection
import java.lang.management.{ManagementFactory, MemoryMXBean, MemoryUsage}

/**
 * Core metric types
 */
sealed trait Metric {
  val name: String
  val tags: Map[String, String]
  val description: String
  val unit: String
}

/**
 * Counter metric - tracks values that increase over time
 */
class Counter(
  val name: String,
  val tags: Map[String, String],
  val description: String,
  val unit: String = "count"
) extends Metric {
  private val count = new AtomicLong(0)
  
  def inc(): Unit = count.incrementAndGet()
  def inc(amount: Long): Unit = count.addAndGet(amount)
  def dec(): Unit = count.decrementAndGet()
  def dec(amount: Long): Unit = count.addAndGet(-amount)
  def get(): Long = count.get()
  def reset(): Unit = count.set(0)
}

/**
 * Gauge metric - tracks a single value that can go up and down
 */
class Gauge(
  val name: String,
  val tags: Map[String, String],
  val description: String,
  val unit: String = "value"
) extends Metric {
  private val value = new AtomicLong(0)
  
  def set(v: Long): Unit = value.set(v)
  def set(v: Double): Unit = value.set(java.lang.Double.doubleToRawLongBits(v))
  def get(): Long = value.get()
  def getAsDouble(): Double = java.lang.Double.longBitsToDouble(value.get())
}

/**
 * Timer metric - tracks durations of operations
 */
class Timer(
  val name: String,
  val tags: Map[String, String],
  val description: String,
  val unit: String = "milliseconds"
) extends Metric {
  private val count = new AtomicLong(0)
  private val totalTime = new AtomicLong(0)
  private val minTime = new AtomicLong(java.lang.Long.MAX_VALUE)
  private val maxTime = new AtomicLong(0)
  private val sumSquares = new AtomicLong(0) // For standard deviation calculation
  
  private val recentTimes = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
  private val maxRecentSamples = 100 // Keep last 100 samples for moving average
  
  def recordNanos(durationNanos: Long): Unit = {
    val durationMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos)
    recordMillis(durationMillis)
  }
  
  def recordMillis(durationMillis: Long): Unit = {
    count.incrementAndGet()
    totalTime.addAndGet(durationMillis)
    
    // Update min/max values
    updateMin(durationMillis)
    updateMax(durationMillis)
    
    // Track sum of squares for variance calculation
    sumSquares.addAndGet(durationMillis * durationMillis)
    
    // Add to recent times queue
    recentTimes.add(durationMillis)
    while (recentTimes.size() > maxRecentSamples) {
      recentTimes.poll()
    }
  }
  
  def time[T](block: => T): T = {
    val start = System.nanoTime()
    try {
      block
    } finally {
      val durationNanos = System.nanoTime() - start
      recordNanos(durationNanos)
    }
  }
  
  private def updateMin(value: Long): Unit = {
    var current = minTime.get()
    while (value < current && !minTime.compareAndSet(current, value)) {
      current = minTime.get()
    }
  }
  
  private def updateMax(value: Long): Unit = {
    var current = maxTime.get()
    while (value > current && !maxTime.compareAndSet(current, value)) {
      current = maxTime.get()
    }
  }
  
  def getCount(): Long = count.get()
  def getTotalTime(): Long = totalTime.get()
  def getAvg(): Double = if (count.get() > 0) totalTime.get().toDouble / count.get() else 0.0
  def getMin(): Long = if (minTime.get() == java.lang.Long.MAX_VALUE) 0 else minTime.get()
  def getMax(): Long = maxTime.get()
  
  def getRecentAvg(): Double = {
    val times = recentTimes.asScala.toSeq
    if (times.isEmpty) 0.0 else times.sum.toDouble / times.size
  }
  
  def getStdDev(): Double = {
    val n = count.get()
    if (n <= 1) return 0.0
    
    val mean = getAvg()
    val variance = (sumSquares.get().toDouble / n) - (mean * mean)
    Math.sqrt(variance)
  }
  
  def reset(): Unit = {
    count.set(0)
    totalTime.set(0)
    minTime.set(java.lang.Long.MAX_VALUE)
    maxTime.set(0)
    sumSquares.set(0)
    recentTimes.clear()
  }
}

/**
 * Histogram metric - tracks distribution of values
 */
class Histogram(
  val name: String,
  val tags: Map[String, String],
  val description: String,
  val unit: String = "value"
) extends Metric {
  private val count = new AtomicLong(0)
  private val sum = new AtomicLong(0)
  private val min = new AtomicLong(java.lang.Long.MAX_VALUE)
  private val max = new AtomicLong(java.lang.Long.MIN_VALUE)
  
  // Bucket boundaries for histogram distribution
  private val buckets = Array(1L, 5L, 10L, 25L, 50L, 100L, 250L, 500L, 1000L, 2500L, 5000L, 10000L)
  private val bucketCounts = buckets.map(_ => new AtomicLong(0))
  private val overflowBucket = new AtomicLong(0)
  
  def update(value: Long): Unit = {
    count.incrementAndGet()
    sum.addAndGet(value)
    
    // Update min/max
    updateMin(value)
    updateMax(value)
    
    // Update buckets
    var bucketFound = false
    for (i <- buckets.indices) {
      if (!bucketFound && value <= buckets(i)) {
        bucketCounts(i).incrementAndGet()
        bucketFound = true
      }
    }
    
    if (!bucketFound) {
      overflowBucket.incrementAndGet()
    }
  }
  
  private def updateMin(value: Long): Unit = {
    var current = min.get()
    while (value < current && !min.compareAndSet(current, value)) {
      current = min.get()
    }
  }
  
  private def updateMax(value: Long): Unit = {
    var current = max.get()
    while (value > current && !max.compareAndSet(current, value)) {
      current = max.get()
    }
  }
  
  def getCount(): Long = count.get()
  def getSum(): Long = sum.get()
  def getMin(): Long = if (min.get() == java.lang.Long.MAX_VALUE) 0 else min.get()
  def getMax(): Long = if (max.get() == java.lang.Long.MIN_VALUE) 0 else max.get()
  def getAvg(): Double = if (count.get() > 0) sum.get().toDouble / count.get() else 0.0
  
  def getBucketCounts(): Map[Long, Long] = {
    val result = buckets.zipWithIndex.map { case (boundary, index) =>
      boundary -> bucketCounts(index).get()
    }.toMap
    
    result + (java.lang.Long.MAX_VALUE -> overflowBucket.get())
  }
  
  def getPercentiles(): Map[Int, Long] = {
    if (count.get() == 0) {
      return Map(50 -> 0L, 75 -> 0L, 90 -> 0L, 95 -> 0L, 99 -> 0L)
    }
    
    // Simple approximation of percentiles using buckets
    // For more precision, a more sophisticated algorithm would be needed
    val total = count.get()
    val runningCounts = new Array[Long](buckets.length + 1)
    var sum = 0L
    
    for (i <- buckets.indices) {
      sum += bucketCounts(i).get()
      runningCounts(i) = sum
    }
    runningCounts(buckets.length) = sum + overflowBucket.get()
    
    val percentiles = Map(
      50 -> findPercentileBucket(runningCounts, 0.5),
      75 -> findPercentileBucket(runningCounts, 0.75),
      90 -> findPercentileBucket(runningCounts, 0.9),
      95 -> findPercentileBucket(runningCounts, 0.95),
      99 -> findPercentileBucket(runningCounts, 0.99)
    )
    
    percentiles
  }
  
  private def findPercentileBucket(runningCounts: Array[Long], percentile: Double): Long = {
    val target = Math.ceil(percentile * count.get()).toLong
    var i = 0
    while (i < runningCounts.length && runningCounts(i) < target) {
      i += 1
    }
    
    if (i >= buckets.length) java.lang.Long.MAX_VALUE else buckets(i)
  }
  
  def reset(): Unit = {
    count.set(0)
    sum.set(0)
    min.set(java.lang.Long.MAX_VALUE)
    max.set(java.lang.Long.MIN_VALUE)
    bucketCounts.foreach(_.set(0))
    overflowBucket.set(0)
  }
}

/**
 * Main metrics registry for collecting and reporting metrics
 */
object MetricsRegistry {
  private val logger = LoggerFactory.getLogger(getClass)
  private val registry = new ConcurrentHashMap[String, Metric]()
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private var initialized = false
  private var reportingEnabled = true
  
  // Metadata for standardized naming and organization
  case class MetricMetadata(
    component: String,
    metricType: String, 
    metricName: String, 
    tags: Map[String, String] = Map.empty
  )
  
  /**
   * Initialize metrics registry
   */
  def init(): Unit = synchronized {
    if (initialized) return
    
    logger.info("Initializing metrics registry")
    
    // Register JVM metrics
    registerJvmMetrics()
    
    // Start periodic reporter
    try {
      val config = ConfigurationService.getInstance.getConfigSection(new MetricsConfigSection(_))
      val reportingInterval = config.reportingIntervalSeconds
      
      if (reportingEnabled && reportingInterval > 0) {
        scheduler.scheduleAtFixedRate(
          () => reportMetrics(),
          reportingInterval,
          reportingInterval,
          TimeUnit.SECONDS
        )
        logger.info(s"Metrics reporting scheduled every $reportingInterval seconds")
      }
    } catch {
      case e: Exception =>
        logger.warn("Failed to read metrics configuration, using defaults", e)
        // Default reporting interval
        scheduler.scheduleAtFixedRate(
          () => reportMetrics(),
          60,
          60,
          TimeUnit.SECONDS
        )
    }
    
    initialized = true
  }
  
  /**
   * Register built-in JVM metrics
   */
  private def registerJvmMetrics(): Unit = {
    // Memory metrics
    val memoryMXBean = ManagementFactory.getMemoryMXBean
    
    // Heap memory usage
    val heapMemoryGauge = buildGauge(
      MetricMetadata("jvm", "memory", "heap.used"),
      "Current heap memory used",
      "bytes"
    )
    
    val heapMaxGauge = buildGauge(
      MetricMetadata("jvm", "memory", "heap.max"),
      "Maximum heap memory",
      "bytes"
    )
    
    // Non-heap memory usage
    val nonHeapMemoryGauge = buildGauge(
      MetricMetadata("jvm", "memory", "nonheap.used"),
      "Current non-heap memory used",
      "bytes"
    )
    
    // Thread metrics
    val threadMXBean = ManagementFactory.getThreadMXBean
    
    val threadCountGauge = buildGauge(
      MetricMetadata("jvm", "threads", "count"),
      "Current thread count",
      "threads"
    )
    
    val peakThreadCountGauge = buildGauge(
      MetricMetadata("jvm", "threads", "peak"),
      "Peak thread count",
      "threads"
    )
    
    // GC metrics
    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans
    gcBeans.asScala.foreach { gcBean =>
      val gcName = gcBean.getName.replace(" ", "_").toLowerCase
      
      val gcCountGauge = buildGauge(
        MetricMetadata("jvm", "gc", s"$gcName.count"),
        s"GC collection count for $gcName",
        "collections"
      )
      
      val gcTimeGauge = buildGauge(
        MetricMetadata("jvm", "gc", s"$gcName.time"),
        s"GC collection time for $gcName",
        "milliseconds"
      )
    }
    
    // Schedule JVM metrics update
    scheduler.scheduleAtFixedRate(
      () => {
        try {
          // Update memory metrics
          val heapUsage = memoryMXBean.getHeapMemoryUsage
          heapMemoryGauge.set(heapUsage.getUsed)
          heapMaxGauge.set(heapUsage.getMax)
          
          val nonHeapUsage = memoryMXBean.getNonHeapMemoryUsage
          nonHeapMemoryGauge.set(nonHeapUsage.getUsed)
          
          // Update thread metrics
          threadCountGauge.set(threadMXBean.getThreadCount)
          peakThreadCountGauge.set(threadMXBean.getPeakThreadCount)
          
          // Update GC metrics
          gcBeans.asScala.foreach { gcBean =>
            val gcName = gcBean.getName.replace(" ", "_").toLowerCase
            
            val gcCountMetric = getGauge(MetricMetadata("jvm", "gc", s"$gcName.count"))
            val gcTimeMetric = getGauge(MetricMetadata("jvm", "gc", s"$gcName.time"))
            
            gcCountMetric.foreach(_.set(gcBean.getCollectionCount))
            gcTimeMetric.foreach(_.set(gcBean.getCollectionTime))
          }
        } catch {
          case e: Exception => 
            logger.warn("Failed to update JVM metrics", e)
        }
      },
      5, 5, TimeUnit.SECONDS
    )
  }
  
  /**
   * Get a counter metric, creating it if it doesn't exist
   */
  def counter(metadata: MetricMetadata, description: String, unit: String = "count"): Counter = {
    val metricKey = formatMetricKey(metadata)
    
    registry.computeIfAbsent(
      metricKey,
      _ => new Counter(metricKey, metadata.tags, description, unit)
    ).asInstanceOf[Counter]
  }
  
  /**
   * Get a gauge metric, creating it if it doesn't exist
   */
  def gauge(metadata: MetricMetadata, description: String, unit: String = "value"): Gauge = {
    val metricKey = formatMetricKey(metadata)
    
    registry.computeIfAbsent(
      metricKey,
      _ => new Gauge(metricKey, metadata.tags, description, unit)
    ).asInstanceOf[Gauge]
  }
  
  /**
   * Get a timer metric, creating it if it doesn't exist
   */
  def timer(metadata: MetricMetadata, description: String, unit: String = "milliseconds"): Timer = {
    val metricKey = formatMetricKey(metadata)
    
    registry.computeIfAbsent(
      metricKey,
      _ => new Timer(metricKey, metadata.tags, description, unit)
    ).asInstanceOf[Timer]
  }
  
  /**
   * Get a histogram metric, creating it if it doesn't exist
   */
  def histogram(metadata: MetricMetadata, description: String, unit: String = "value"): Histogram = {
    val metricKey = formatMetricKey(metadata)
    
    registry.computeIfAbsent(
      metricKey,
      _ => new Histogram(metricKey, metadata.tags, description, unit)
    ).asInstanceOf[Histogram]
  }
  
  /**
   * Helper method to build and register a counter
   */
  def buildCounter(metadata: MetricMetadata, description: String, unit: String = "count"): Counter = {
    val metricKey = formatMetricKey(metadata)
    val counter = new Counter(metricKey, metadata.tags, description, unit)
    registry.put(metricKey, counter)
    counter
  }
  
  /**
   * Helper method to build and register a gauge
   */
  def buildGauge(metadata: MetricMetadata, description: String, unit: String = "value"): Gauge = {
    val metricKey = formatMetricKey(metadata)
    val gauge = new Gauge(metricKey, metadata.tags, description, unit)
    registry.put(metricKey, gauge)
    gauge
  }
  
  /**
   * Helper method to build and register a timer
   */
  def buildTimer(metadata: MetricMetadata, description: String, unit: String = "milliseconds"): Timer = {
    val metricKey = formatMetricKey(metadata)
    val timer = new Timer(metricKey, metadata.tags, description, unit)
    registry.put(metricKey, timer)
    timer
  }
  
  /**
   * Helper method to build and register a histogram
   */
  def buildHistogram(metadata: MetricMetadata, description: String, unit: String = "value"): Histogram = {
    val metricKey = formatMetricKey(metadata)
    val histogram = new Histogram(metricKey, metadata.tags, description, unit)
    registry.put(metricKey, histogram)
    histogram
  }
  
  /**
   * Get a counter by metadata
   */
  def getCounter(metadata: MetricMetadata): Option[Counter] = {
    val metricKey = formatMetricKey(metadata)
    Option(registry.get(metricKey)) match {
      case Some(counter: Counter) => Some(counter)
      case _ => None
    }
  }
  
  /**
   * Get a gauge by metadata
   */
  def getGauge(metadata: MetricMetadata): Option[Gauge] = {
    val metricKey = formatMetricKey(metadata)
    Option(registry.get(metricKey)) match {
      case Some(gauge: Gauge) => Some(gauge)
      case _ => None
    }
  }
  
  /**
   * Get a timer by metadata
   */
  def getTimer(metadata: MetricMetadata): Option[Timer] = {
    val metricKey = formatMetricKey(metadata)
    Option(registry.get(metricKey)) match {
      case Some(timer: Timer) => Some(timer)
      case _ => None
    }
  }
  
  /**
   * Get a histogram by metadata
   */
  def getHistogram(metadata: MetricMetadata): Option[Histogram] = {
    val metricKey = formatMetricKey(metadata)
    Option(registry.get(metricKey)) match {
      case Some(histogram: Histogram) => Some(histogram)
      case _ => None
    }
  }
  
  /**
   * Format a metric key for consistent naming
   */
  private def formatMetricKey(metadata: MetricMetadata): String = {
    val baseName = s"${metadata.component}.${metadata.metricType}.${metadata.metricName}"
    
    if (metadata.tags.isEmpty) {
      baseName
    } else {
      val tagString = metadata.tags.map { case (k, v) => s"$k=$v" }.mkString(",")
      s"$baseName{$tagString}"
    }
  }
  
  /**
   * Parse a metric key into its components
   */
  def parseMetricKey(key: String): Option[MetricMetadata] = {
    try {
      val tagPattern = """\{(.+?)\}$""".r
      val tagsMap = tagPattern.findFirstMatchIn(key) match {
        case Some(m) =>
          val tagString = m.group(1)
          tagString.split(",").map { tag =>
            val parts = tag.split("=")
            parts(0) -> parts(1)
          }.toMap
        case None => Map.empty[String, String]
      }
      
      val nameWithoutTags = tagPattern.replaceFirstIn(key, "")
      val parts = nameWithoutTags.split("\\.")
      
      if (parts.length >= 3) {
        val component = parts(0)
        val metricType = parts(1)
        val metricName = parts.drop(2).mkString(".")
        
        Some(MetricMetadata(component, metricType, metricName, tagsMap))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }
  
  /**
   * Report all metrics to logs
   */
  def reportMetrics(): Unit = {
    if (!reportingEnabled) return
    
    try {
      // Group metrics by component for better organization in logs
      val metricsByComponent = MutableMap[String, MutableMap[String, Metric]]()
      
      registry.asScala.foreach { case (key, metric) =>
        parseMetricKey(key).foreach { metadata =>
          val componentMap = metricsByComponent.getOrElseUpdate(metadata.component, MutableMap[String, Metric]())
          componentMap += (key -> metric)
        }
      }
      
      // Log metrics by component
      metricsByComponent.keys.toSeq.sorted.foreach { component =>
        val componentMetrics = metricsByComponent(component)
        val sb = new StringBuilder(s"\n=== Metrics for component: $component ===\n")
        
        componentMetrics.keys.toSeq.sorted.foreach { key =>
          val metric = componentMetrics(key)
          sb.append(formatMetric(key, metric))
        }
        
        logger.info(sb.toString)
      }
    } catch {
      case e: Exception =>
        logger.error("Error reporting metrics", e)
    }
  }
  
  /**
   * Format a metric for display
   */
  private def formatMetric(key: String, metric: Metric): String = {
    metric match {
      case counter: Counter =>
        f"$key: ${counter.get()}%d ${counter.unit}\n"
        
      case gauge: Gauge =>
        if (gauge.unit == "bytes") {
          f"$key: ${formatBytes(gauge.get())} ${gauge.unit}\n"
        } else {
          f"$key: ${gauge.get()}%d ${gauge.unit}\n"
        }
        
      case timer: Timer =>
        f"$key: count=${timer.getCount()}%d, avg=${timer.getAvg()}%.2f, " +
        f"min=${timer.getMin()}%d, max=${timer.getMax()}%d, " +
        f"recent_avg=${timer.getRecentAvg()}%.2f ${timer.unit}\n"
        
      case histogram: Histogram =>
        val percentiles = histogram.getPercentiles()
        f"$key: count=${histogram.getCount()}%d, avg=${histogram.getAvg()}%.2f, " +
        f"min=${histogram.getMin()}%d, max=${histogram.getMax()}%d, " +
        f"p50=${percentiles(50)}%d, p95=${percentiles(95)}%d, p99=${percentiles(99)}%d ${histogram.unit}\n"
        
      case _ => s"$key: [Unknown metric type]\n"
    }
  }
  
  /**
   * Format bytes for better readability
   */
  private def formatBytes(bytes: Long): String = {
    if (bytes < 1024) return s"$bytes B"
    val exp = (Math.log(bytes) / Math.log(1024)).toInt
    val pre = "KMGTPE".charAt(exp - 1)
    f"${bytes / Math.pow(1024, exp)}%.1f ${pre}B"
  }
  
  /**
   * Get all metrics for a specific component
   */
  def getMetricsForComponent(component: String): Map[String, Metric] = {
    registry.asScala.filter { case (key, _) =>
      parseMetricKey(key).exists(_.component == component)
    }.toMap
  }
  
  /**
   * Reset a specific metric
   */
  def resetMetric(key: String): Unit = {
    Option(registry.get(key)).foreach {
      case counter: Counter => counter.reset()
      case timer: Timer => timer.reset()
      case histogram: Histogram => histogram.reset()
      case _ => // Cannot reset gauges
    }
  }
  
  /**
   * Reset all metrics for a component
   */
  def resetComponentMetrics(component: String): Unit = {
    getMetricsForComponent(component).keys.foreach(resetMetric)
  }
  
  /**
   * Reset all metrics
   */
  def resetAllMetrics(): Unit = {
    registry.asScala.keys.foreach(resetMetric)
  }
  
  /**
   * Remove a specific metric
   */
  def removeMetric(key: String): Unit = {
    registry.remove(key)
  }
  
  /**
   * Get all metrics in the registry
   */
  def getAllMetrics(): Map[String, Metric] = {
    registry.asScala.toMap
  }
  
  /**
   * Enable or disable metrics reporting
   */
  def setReportingEnabled(enabled: Boolean): Unit = {
    reportingEnabled = enabled
  }
  
  /**
   * Check if the metrics registry has been initialized
   */
  def isInitialized: Boolean = initialized
  
  /**
   * Shutdown metrics registry
   */
  def shutdown(): Unit = {
    logger.info("Shutting down metrics registry")
    scheduler.shutdown()
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow()
      }
    } catch {
      case _: InterruptedException => 
        scheduler.shutdownNow()
    }
  }
}