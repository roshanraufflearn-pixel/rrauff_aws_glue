import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import year, to_date

# Job setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 1: Read cleaned customer data (Parquet from Job 1)
customers_df = spark.read.parquet("s3://rrauff-awsglue/output/")

# Step 2: Read transactions CSV from Glue Catalog
transactions_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="customer360_pg",
    table_name="transactions_csv"
)
transactions_df = transactions_dyf.toDF()

# Step 3: Read geolocation metadata JSON from Glue Catalog
geo_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="customer360_pg",
    table_name="geolocation_dataset_json"
)
geo_df = geo_dyf.toDF()



# Step 4: Join customers and transactions on customer_id
cust_txn_df = customers_df.join(transactions_df, on="customer_id", how="inner")

geo_df = geo_df.withColumnRenamed("zip_code", "geo_zip_code")

# Step 5: Join with geolocation on zip_code
cust_txn_geo_df = cust_txn_df.join(geo_df, cust_txn_df["zip_code"] == geo_df["geo_zip_code"], how="left")


# Step 6: Aggregate total_transaction_amount and transaction_count
agg_df = cust_txn_geo_df.groupBy(
    "customer_id", "name", "zip_code", "city", "state"
).agg(
    F.sum("transaction_amount").alias("total_transaction_amount"),
    F.count("transaction_id").alias("transaction_count"),
    F.first("transaction_date").alias("latest_transaction_date")  # needed for year partitioning
)

# Step 7: Extract year for partitioning
agg_df = agg_df.withColumn("year", year(to_date("latest_transaction_date")))

# Step 8: Write final output to S3 in Parquet, partitioned by state and year
agg_df.write.mode("overwrite") \
    .partitionBy("state", "year") \
    .option("compression", "snappy") \
    .parquet("s3://rrauff-awsglue/output/final/")

job.commit()
