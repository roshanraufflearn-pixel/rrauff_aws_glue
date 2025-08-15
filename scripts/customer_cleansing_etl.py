import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# Step 1: Read customers.csv from Glue Catalog
customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="customer360_pg",
    table_name="customers_dataset_csv",
    transformation_ctx="customers_dyf"
)
customers_df = customers_dyf.toDF()


# Step 2: Remove duplicates and filter nulls
cleaned_df = customers_df.dropDuplicates(["customer_id"]) \
                         .filter((F.col("email").isNotNull()) & (F.col("zip_code").isNotNull()))

# Step 3: Write cleaned data to S3 in Parquet format
output_path = "s3://rrauff-awsglue/output/"  # Replace with your actual bucket path

cleaned_df.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()