"""
================================================================================
AWS Glue 4.0 Serverless Apache Spark ETL Job: DTCC SDR Market Surveillance
================================================================================
Engine: AWS Glue 4.0 (Apache Spark 3.3, Python 3.10)
Role: Principal AWS Data Engineer
Layer: Bronze (S3 Parquet / Iceberg) -> Silver (Curated Market Surveillance)

Description:
    Serverless PySpark distributed transformation job executing across multi-node
    Glue worker clusters (G.1X / G.2X). Reads raw partitioned DTCC SDR swap trades
    from S3 Sydney, performs distributed aggregations, calculates clearing penetration,
    detects block trades (> $50M notional), and writes curated Silver analytical tables.

Target S3 Destinations:
    Source: s3://dtcc-lakehouse-honey-2026/warehouse/bronze/dtcc_sdr_swaps/data/
    Target: s3://dtcc-lakehouse-honey-2026/warehouse/silver/glue_market_surveillance/
================================================================================
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DecimalType

# 1. Initialize AWS Glue Spark Context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"[INFO] Initialized AWS Glue Spark ETL Job: {args['JOB_NAME']}")

# 2. Define S3 Locations
SOURCE_S3_PATH = "s3://dtcc-lakehouse-honey-2026/warehouse/bronze/dtcc_sdr_swaps/data/"
TARGET_S3_PATH = "s3://dtcc-lakehouse-honey-2026/warehouse/silver/glue_market_surveillance/"

# 3. Read Bronze S3 Parquet Data
print(f"[INFO] Reading Bronze Parquet data from {SOURCE_S3_PATH}...")
df_bronze = spark.read.parquet(SOURCE_S3_PATH)

total_input_records = df_bronze.count()
print(f"[INFO] Successfully loaded {total_input_records:,} raw trade records.")

# 4. Cleansing & Feature Engineering
df_cleansed = df_bronze.filter(
    F.col("dissemination_id").isNotNull() &
    F.col("asset_class").isNotNull()
).withColumn(
    "notional_usd",
    F.coalesce(F.col("notional_amount_1").cast(DoubleType()), F.lit(0.0))
).withColumn(
    "notional_millions",
    F.round(F.col("notional_usd") / 1e6, 4)
).withColumn(
    "is_cleared",
    F.when(F.upper(F.col("cleared")) == "C", 1).otherwise(0)
).withColumn(
    "is_large_block",
    F.when(F.col("notional_usd") >= 50000000.0, 1).otherwise(0)
)

# 5. Distributed Aggregation: Market Surveillance by Asset Class & Clearing Status
df_surveillance = df_cleansed.groupBy("asset_class", "cleared").agg(
    F.count("*").alias("total_trades"),
    F.round(F.sum("notional_millions"), 2).alias("total_notional_millions"),
    F.round(F.avg("notional_millions"), 2).alias("avg_trade_size_millions"),
    F.round(F.max("notional_millions"), 2).alias("max_trade_size_millions"),
    F.sum("is_large_block").alias("large_block_trade_count"),
    F.current_timestamp().alias("processed_timestamp")
).orderBy(F.col("total_notional_millions").desc())

print("[INFO] Sample Curated Market Surveillance Metrics:")
df_surveillance.show(truncate=False)

# 6. Write Silver Curated Parquet to S3 with Snappy/ZSTD Compression
print(f"[INFO] Writing curated Silver metrics to {TARGET_S3_PATH}...")
(
    df_surveillance
    .repartition(1)
    .write
    .mode("overwrite")
    .parquet(TARGET_S3_PATH)
)

print(f"[SUCCESS] Curated Silver data successfully written to {TARGET_S3_PATH}")

# 7. Commit Glue Job
job.commit()
