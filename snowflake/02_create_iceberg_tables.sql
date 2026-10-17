-- ==============================================================================
-- SNOWFLAKE INTEGRATION STEP 2: CREATE & MAP ICEBERG TABLES
-- ==============================================================================
-- Role: Principal AWS Data Engineer
-- Context: Zero-Copy Querying of Apache Iceberg from Snowflake
-- Key Concept:
--   Snowflake creates an ICEBERG TABLE referencing the S3 External Volume
--   and Polaris Catalog. No data is copied into Snowflake internal storage.
-- ==============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DTCC_LAKEHOUSE;
USE SCHEMA BRONZE;

-- ==============================================================================
-- METHOD A: AUTOMATED SYNC VIA APACHE POLARIS REST CATALOG
-- ==============================================================================
-- Used when Polaris is accessible to Snowflake over public or VPC network.
-- Snowflake queries Polaris to resolve the current Iceberg snapshot metadata.
-- ==============================================================================
CREATE OR REPLACE ICEBERG TABLE DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps
  EXTERNAL_VOLUME = 'dtcc_s3_volume'
  CATALOG = 'polaris_catalog_integration'
  CATALOG_TABLE_NAME = 'dtcc_sdr_swaps';

-- Explicitly trigger snapshot refresh from Polaris (or configure automated auto-refresh)
ALTER ICEBERG TABLE DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps REFRESH;

-- ==============================================================================
-- METHOD B: DIRECT METADATA POINTER (STUDENT / LOCAL SANDBOX COMPATIBILITY)
-- ==============================================================================
-- Teaching Note:
-- When running Polaris on localhost in Docker, Snowflake Cloud cannot reach
-- your local loopback. Method B maps the table directly to the Iceberg metadata
-- file in your AWS S3 bucket. This guarantees the student can test zero-copy
-- queries in Snowflake without needing a paid reverse proxy or AWS ALB!
-- ==============================================================================
-- Example usage (substitute latest metadata JSON relative path):
-- CREATE OR REPLACE ICEBERG TABLE DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps_direct
--   EXTERNAL_VOLUME = 'dtcc_s3_volume'
--   METADATA_FILE_PATH = 'bronze/dtcc_sdr_swaps/metadata/00000-latest.metadata.json';

-- ==============================================================================
-- 3. VERIFICATION & METADATA INSPECTION
-- ==============================================================================

-- Inspect Table Details & Underlying Iceberg Metadata
DESCRIBE TABLE DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps;

-- Query the official Iceberg snapshot metadata via Snowflake system function
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps');

-- Verify Record Count and Asset Class Distribution
SELECT 
    asset_class,
    cleared,
    COUNT(*) AS total_trades,
    ROUND(SUM(notional_amount_1) / 1e6, 2) AS total_notional_usd_millions,
    MIN(execution_timestamp) AS earliest_execution,
    MAX(execution_timestamp) AS latest_execution
FROM DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps
GROUP BY 1, 2
ORDER BY total_notional_usd_millions DESC;

-- ==============================================================================
-- 4. PARTITION PRUNING DEMONSTRATION (Zero-Copy Cost Savings)
-- ==============================================================================
-- Check Query Profile in Snowflake UI for this query:
-- Because the Iceberg table is partitioned by identity(asset_class) and months(execution_timestamp),
-- Snowflake prunes 90%+ of Parquet data files at the manifest level before scanning S3!
SELECT 
    dissemination_id,
    product_id,
    notional_currency_1,
    notional_amount_1,
    fixed_rate_1,
    spread_1
FROM DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps
WHERE asset_class = 'RATES'
  AND execution_timestamp >= DATEADD('month', -1, CURRENT_TIMESTAMP())
LIMIT 100;
