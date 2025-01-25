import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.logs.AWSLogsClientBuilder
import com.amazonaws.services.logs.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.Try
import org.apache.logging.log4j.LogManager

// Custom CloudWatch Logger
// class CloudWatchLogger(
//     logGroupName: String,
//     logStreamName: String = UUID.randomUUID().toString
// ) {
//   private val cloudWatchClient = AWSLogsClientBuilder.standard().build()
//   private var sequenceToken: Option[String] = None

//   // Create log group and stream if they don't exist
//   Try(cloudWatchClient.createLogGroup(new CreateLogGroupRequest(logGroupName)))
//   Try(cloudWatchClient.createLogStream(
//     new CreateLogStreamRequest().withLogGroupName(logGroupName).withLogStreamName(logStreamName)
//   ))

//   def log(message: String, level: String = "INFO"): Unit = {
//     val timestamp = System.currentTimeMillis()
//     val logEvent = new InputLogEvent().withTimestamp(timestamp).withMessage(s"[$level] $message")
//     val request = new PutLogEventsRequest()
//       .withLogGroupName(logGroupName)
//       .withLogStreamName(logStreamName)
//       .withLogEvents(List(logEvent).asJava)

//     sequenceToken.foreach(request.setSequenceToken)

//     Try {
//       val result = cloudWatchClient.putLogEvents(request)
//       sequenceToken = Option(result.getNextSequenceToken)
//     }.recover {
//       case e: InvalidSequenceTokenException =>
//         request.setSequenceToken(e.getExpectedSequenceToken)
//         val result = cloudWatchClient.putLogEvents(request)
//         sequenceToken = Option(result.getNextSequenceToken)
//     }
//   }

//   def info(message: String): Unit = log(message, "INFO")
//   def error(message: String): Unit = log(message, "ERROR")
//   def warn(message: String): Unit = log(message, "WARN")
//   def debug(message: String): Unit = log(message, "DEBUG")
// }

object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    // // Initialize custom logger
    // val logger = new CloudWatchLogger(
    //   logGroupName = "MyGlueJob_Logs",
    //   logStreamName = s"glue-job-${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))}"
    // )


    val logger = LogManager.getLogger(GlueApp.getClass)


    val startTime = System.currentTimeMillis()

    try {
      // Parse arguments
      val args = GlueArgParser.getResolvedOptions(sysArgs, Array("JOB_NAME", "SOURCE_PATH", "DESTINATION_PATH"))
      val sourcePath = args("SOURCE_PATH")
      val destinationPath = args("DESTINATION_PATH")

      logger.info(s"Starting Glue job: ${args("JOB_NAME")}")
      logger.info(s"Source path: $sourcePath")
      logger.info(s"Destination path: $destinationPath")

      // Initialize GlueContext
      val spark: SparkContext = new SparkContext()
      val glueContext = new GlueContext(spark)
      val sparkSession: SparkSession = glueContext.getSparkSession
      Job.init(args("JOB_NAME"), glueContext, args.asJava)

      // Read source data
      logger.info("Reading source data...")
      val sourceData = sparkSession.read
        .option("header", "true")
        .format("csv")
        .load(sourcePath)

      val recordCount = sourceData.count()
      logger.info(s"Read $recordCount records from source")

      // Log data quality
      logDataQuality(sourceData, logger)

      // Transform data
      logger.info("Performing transformations...")
      val transformedDF = sourceData.withColumnRenamed("shortId", "PID")

      // Write data to destination
      logger.info(s"Writing data to destination: $destinationPath")
      transformedDF.write
        .format("csv")
        .option("quote", null)
        .option("header", "true")
        .mode("overwrite")
        .save(destinationPath)

      // Log performance metrics
      logPerformanceMetrics(startTime, recordCount, logger)

      logger.info("Job completed successfully.")
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
  def logDataQuality(df: org.apache.spark.sql.DataFrame, logger: org.apache.logging.log4j.Logger): Unit = {
    logger.info("Running Data Quality Checks:")
    df.columns.foreach { column =>
      val nullCount = df.filter(df(column).isNull).count()
      val totalCount = df.count()
      val nullPercentage = (nullCount.toDouble / totalCount) * 100
      logger.info(f"Column: $column, Null Count: $nullCount, Null Percentage: $nullPercentage%.2f%%")
    }
  }

  // Helper method for performance logging
  def logPerformanceMetrics(startTime: Long, recordCount: Long, logger: org.apache.logging.log4j.Logger): Unit = {
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
