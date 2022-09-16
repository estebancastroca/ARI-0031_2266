import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="datos_clima",
    table_name="asi_ha_aumentado_la_concentracion_de_co2_en_la_atmosfera",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1663207200893 = DynamicFrame.fromDF(
    S3bucket_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1663207200893",
)

# Script generated for node Amazon S3
AmazonS3_node1663207237953 = glueContext.getSink(
    path="s3://testbucketari-2022/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="lzo",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1663207237953",
)
AmazonS3_node1663207237953.setCatalogInfo(
    catalogDatabase="datos_clima",
    catalogTableName="clean_asi_ha_aumentado_la_concentracion_de_co2_en_la_atmosfera",
)
AmazonS3_node1663207237953.setFormat("glueparquet")
AmazonS3_node1663207237953.writeFrame(DropDuplicates_node1663207200893)
job.commit()
