-- ==============================================================================
-- SNOWFLAKE INTEGRATION STEP 1: EXTERNAL VOLUME & POLARIS REST CATALOG SETUP
-- ==============================================================================
-- Role: Principal AWS Data Engineer
-- Context: Zero-Copy Open Lakehouse Architecture
-- Objective:
--   Configure Snowflake to read Apache Iceberg tables residing in AWS S3
--   governed by Apache Polaris REST Catalog. Snowflake accesses the Parquet files
--   directly without copying, copying zero data into Snowflake internal storage!
-- ==============================================================================

-- 1. Use ACCOUNTADMIN role to configure integrations and storage volumes
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- 2. Create target database and schemas
CREATE DATABASE IF NOT EXISTS DTCC_LAKEHOUSE
  COMMENT = 'DTCC Swap Data Repository Zero-Copy Open Lakehouse';

CREATE SCHEMA IF NOT EXISTS DTCC_LAKEHOUSE.BRONZE
  COMMENT = 'Bronze Layer: External Iceberg Tables mapped via Polaris';

CREATE SCHEMA IF NOT EXISTS DTCC_LAKEHOUSE.SILVER
  COMMENT = 'Silver Layer: Cleaned, Curated, Deduplicated Swaps';

CREATE SCHEMA IF NOT EXISTS DTCC_LAKEHOUSE.GOLD
  COMMENT = 'Gold Layer: Analytics, Counterparty Risk, Anomaly Detection Marts';

-- ==============================================================================
-- 3. CREATE EXTERNAL VOLUME (Storage Connector to AWS S3)
-- ==============================================================================
-- An External Volume represents the cloud storage location where Iceberg data
-- files (Parquet) and metadata files reside.
-- ==============================================================================
CREATE OR REPLACE EXTERNAL VOLUME dtcc_s3_volume
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'dtcc-s3-us-east-1'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://dtcc-open-lakehouse-bronze/iceberg/warehouse/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3ExternalVolumeRole'
      )
    )
  COMMENT = 'External storage volume pointing to AWS S3 Iceberg warehouse';

-- ==============================================================================
-- 4. RETRIEVE AWS IAM TRUST RELATIONSHIP PARAMETERS
-- ==============================================================================
-- RUN THIS QUERY TO OBTAIN:
--   - STORAGE_AWS_IAM_USER_ARN
--   - STORAGE_AWS_EXTERNAL_ID
-- You must update your AWS IAM Role Trust Policy with these exact values:
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN>" },
--       "Action": "sts:AssumeRole",
--       "Condition": {
--         "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>" }
--       }
--     }
--   ]
-- }
-- ==============================================================================
DESCRIBE EXTERNAL VOLUME dtcc_s3_volume;

-- ==============================================================================
-- 5. CREATE CATALOG INTEGRATION (Apache Polaris REST Catalog Connector)
-- ==============================================================================
-- A Catalog Integration connects Snowflake to Apache Polaris so Snowflake
-- dynamically tracks new snapshots, schema evolution, and partition pruning specs.
-- ==============================================================================
CREATE OR REPLACE CATALOG INTEGRATION polaris_catalog_integration
  CATALOG_SOURCE = POLARIS
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'bronze'
  REST_CONFIG = (
    CATALOG_URI = 'http://localhost:8181/api/catalog/v1'  -- Or public/private DNS of Polaris
    WAREHOUSE = 'dtcc_catalog'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = 'polaris-admin'
    OAUTH_CLIENT_SECRET = 'polaris-secret'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
  )
  ENABLED = TRUE
  COMMENT = 'Snowflake integration with Apache Polaris Iceberg REST Catalog';

-- Verify Catalog Integration configuration
DESCRIBE CATALOG INTEGRATION polaris_catalog_integration;
