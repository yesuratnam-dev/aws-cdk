import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import com.amazonaws.services.glue.dq.EvaluateDataQuality
import org.apache.logging.log4j.{LogManager, Logger, Level}
import org.apache.logging.log4j.core.config.Configurator

object GlueApp {
  def main(sysArgs: Array[String]) {
    
    
    Configurator.setRootLevel(Level.WARN)
    
    // Only customize specific loggers that need different levels
    
    val logger: Logger = LogManager.getLogger(GlueApp.getClass)
    Configurator.setLevel(logger.getName(), Level.INFO) 
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    
     
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    logger.info("reading data...")
    
    
    logger.info("Data transformation completed")
    logger.warn("Memory usage high")
    logger.error("Failed to connect to database")
    logger.debug("Debug data...")



    // Default ruleset used by all target nodes with data quality enabled
    val DEFAULT_DATA_QUALITY_RULESET = """
        Rules = [
            ColumnCount > 0
        ]
    """
    logger.error(s"Job failed with error: ")
    
    
    // Script generated for node Amazon S3
    val AmazonS3_node1737791164399 = glueContext.getSourceWithFormat(formatOptions=JsonOptions("""{"quoteChar": "\"", "withHeader": true, "separator": ","}"""), connectionType="s3", format="csv", options=JsonOptions("""{"paths": ["s3://kuyesura-dev-9"], "recurse": true}"""), transformationContext="AmazonS3_node1737791164399").getDynamicFrame()

    // Script generated for node Amazon S3
    EvaluateDataQuality.processRows(frame=AmazonS3_node1737791164399, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishingOptions=JsonOptions("""{"dataQualityEvaluationContext": "EvaluateDataQuality_node1737791049339", "enableDataQualityResultsPublishing": "true"}"""), additionalOptions=JsonOptions("""{"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}"""))
    val AmazonS3_node1737791296727 = glueContext.getSinkWithFormat(connectionType="s3", options=JsonOptions("""{"path": "s3://kuyesura-dev-9", "partitionKeys": []}"""), formatOptions=JsonOptions("""{"compression": "snappy"}"""), transformationContext="AmazonS3_node1737791296727", format="glueparquet").writeDynamicFrame(AmazonS3_node1737791164399)
    logger.info("completed data...")
    
    
    Job.commit()
  }
}