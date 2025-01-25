import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from datetime import datetime
import logging

import subprocess
# Install watchtower at runtime
subprocess.check_call([sys.executable, "-m", "pip", "install", "watchtower"])


import watchtower  # Add this import

# Configure CloudWatch logging
logger = logging.getLogger('MyGlueJob_Logs')
logger.setLevel(logging.INFO)

# Create CloudWatch handler
handler = watchtower.CloudWatchLogHandler(
    log_group_name='MyGlueJob_Logs',  # Your custom log group name
    stream_name=f'glue-job-{datetime.now().strftime("%Y%m%d-%H%M%S")}',  # Unique stream name
    use_queues=False  # Immediate logging
)
logger.addHandler(handler)

try:
    # Initialize Glue context and Spark context
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    ### Variables
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # S3 source and destination
    source_path = "s3://kuyesura-dev-9/"
    destination_path = "s3://kuyesura-dev-9-destination-bucket/"

    logger.info('Starting Glue job execution')

    # Read data from S3
    logger.info('Reading data from s3')
    source_data = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [source_path]},
        format="csv",
        format_options={"withHeader": True}
    )

    # Transformation
    logger.info('Transforming data')
    df = source_data.toDF()
    transformed_df = df.withColumnRenamed("shortId", "PID")

    # Convert back to DynamicFrame
    transformed_dynamic_frame = DynamicFrame.fromDF(
        transformed_df, glueContext, "convert"
    )

    # Write data back to S3
    logger.info('Writing data to s3')
    glueContext.write_dynamic_frame.from_options(
        frame=transformed_dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"{destination_path}_{timestamp}"},
        format="csv",
        format_options={"withHeader": True}
    )

    logger.info('Glue job execution completed')
    
except Exception as e:
    logger.error(f"Error in job execution: {str(e)}")
    raise
    
finally:
    # Important: Close the CloudWatch handler to ensure all logs are sent
    handler.close()
    job.commit()