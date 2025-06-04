import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746854753443 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1746854753443")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1746854564565 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="cleaned_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog_node1746854564565")

# Script generated for node Join
Join_node1746854802917 = Join.apply(frame1=AWSGlueDataCatalog_node1746854753443, frame2=AWSGlueDataCatalog_node1746854564565, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1746854802917")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1746854802917, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746854278844", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746856147080 = glueContext.getSink(path="s3://ds-youtube-analytics-uswest2-dev", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746856147080")
AmazonS3_node1746856147080.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final_analytics")
AmazonS3_node1746856147080.setFormat("glueparquet", compression="snappy")
AmazonS3_node1746856147080.writeFrame(Join_node1746854802917)
job.commit()
