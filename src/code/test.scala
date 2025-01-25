import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.DynamicFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.amazonaws.services.logs.AWSLogsClientBuilder
import com.amazonaws.services.logs.model._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Properties, UUID}
import scala.collection.JavaConverters._
import scala.util.Try

// Custom CloudWatch Logger
class CloudWatchLogger(
    logGroupName: String,
    logStreamName: String = UUID.randomUUID().toString
) {
  private val cloudWatchClient = AWSLogsClientBuilder.standard().build()
  private var sequenceToken: Option[String] = None

  // Create log group and stream if they don't exist
  Try {
    cloudWatchClient.createLogGroup(new CreateLogGroupRequest(logGroupName))
  }

  Try {
    cloudWatchClient.createLogStream(
      new CreateLogStreamRequest()
        .withLogGroupName(logGroupName)
        .withLogStreamName(logStreamName)
    )
  }

  def log(message: String, level: String = "INFO"): Unit = {
    val timestamp = System.currentTimeMillis()
    val logEvent = new InputLogEvent()
      .withTimestamp(timestamp)
      .withMessage(s"[$level] $message")

    val request = new PutLogEventsRequest()
      .withLogGroupName(logGroupName)
      .withLogStreamName(logStreamName)
      .withLogEvents(List(logEvent).asJava)

    sequenceToken.foreach(request.setSequenceToken)

    Try {
      val result = cloudWatchClient.putLogEvents(request)
      sequenceToken = Option(result.getNextSequenceToken)
    }.recover {
      case e: InvalidSequenceTokenException =>
        request.setSequenceToken(e.getExpectedSequenceToken)
        val result = cloudWatchClient.putLogEvents(request)
        sequenceToken = Option(result.getNextSequenceToken)
    }
  }

  def info(message: String): Unit = log(message, "INFO")
  def error(message: String): Unit = log(message, "ERROR")
  def warn(message: String): Unit = log(message, "WARN")
  def debug(message: String): Unit = log(message, "DEBUG")
}

object GlueApp {
  def main(sysArgs: Array[String]) {
    // Initialize custom logger with MyGlue_Logs group
    val logger = new CloudWatchLogger(
      logGroupName = "MyGlueJob_Logs",
      logStreamName = s"glue-job-${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))}"
    )
    
    try {
      val spark: SparkContext = new SparkContext()
      val glueContext: GlueContext = new GlueContext(spark)
      val sparkSession: SparkSession = glueContext.getSparkSession

      val args = GlueArgParser.getResolvedOptions(sysArgs, Array("JOB_NAME"))
      Job.init(args("JOB_NAME"), glueContext, args.asJava)

      val sourcePath = "s3://kuyesura-dev-9/"
      val destinationPath = "s3://kuyesura-dev-9-destination-bucket/output/"
      
      logger.info(s"Source path: $sourcePath")
      logger.info(s"Destination path: $destinationPath")

      logger.info("Starting Glue job execution")

      logger.info("Reading source data")
      val sourceData = glueContext.getSourceWithFormat(
        connectionType = "s3",
        options = JsonOptions(Map(
          "paths" -> Seq(sourcePath).asJava,
          "recurse" -> "true"
        ).asJava),
        format = "csv",
        formatOptions = JsonOptions(Map(
          "withHeader" -> "true"
        ).asJava)
      ).getDynamicFrame()

      // Get count from DataFrame
      val recordCount = sourceData.toDF().count()
      logger.info(s"Read $recordCount records from source")

      // Transform data
      logger.info("Performing transformations")
      val transformedDF = sourceData.toDF()
        .withColumnRenamed("shortId", "PID")

      val transformedDynamicFrame = DynamicFrame.apply(
        transformedDF,
        glueContext
      )

      // Write data
      logger.info(s"Writing data to destination: $destinationPath")
      
      glueContext.getSinkWithFormat(
        connectionType="s3",
        options=JsonOptions("""{"path": "s3://kuyesura-dev-9-destination-bucket/output/"}"""),
        format="csv"
    ).writeDynamicFrame(transformedDynamicFrame)

      logger.info("Job completed successfully")

    } catch {
      case e: Exception =>
        logger.error(s"Job failed with error: ${e.getMessage}")
        logger.error(s"Stack trace: ${e.getStackTrace.mkString("\n")}")
        throw e
    } finally {
      Job.commit()
    }
  }

  // Helper method for data quality checks
  def logDataQuality(df: org.apache.spark.sql.DataFrame, logger: CloudWatchLogger): Unit = {
    logger.info("Running Data Quality Checks:")
    df.columns.foreach { column =>
      val nullCount = df.filter(df(column).isNull).count()
      val totalCount = df.count()
      val nullPercentage = (nullCount.toDouble / totalCount) * 100
      logger.info(f"Column: $column, Null Count: $nullCount, Null Percentage: $nullPercentage%.2f%%")
    }
  }

  // Helper method for performance logging
  def logPerformanceMetrics(startTime: Long, recordCount: Long, logger: CloudWatchLogger): Unit = {
    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 1000.0
    logger.info(s"""
      |Performance Metrics:
      |Total Records: $recordCount
      |Processing Time: $duration seconds
      |Records per Second: ${recordCount / duration}
      """.stripMargin)
  }
}
