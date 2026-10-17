"""
================================================================================
Snowflake Database, Schema & Bronze Data Loader
================================================================================
Role: Principal AWS Data Engineer
Layer: Snowflake Integration & Ingestion
Description:
    Initializes DTCC_LAKEHOUSE database and BRONZE, SILVER, GOLD schemas in Snowflake.
    Creates the BRONZE.dtcc_sdr_swaps table and stages the 11,500 trade records
    from ./raw_data/*.csv for downstream dbt transformations.
================================================================================
"""

import os
import glob
import logging
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] SnowflakeInit: %(message)s",
)
logger = logging.getLogger("SnowflakeInit")


def init_snowflake():
    sf_account = os.getenv("SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
    sf_user = os.getenv("SNOWFLAKE_USER", "LAKEHOUSE_ADMIN")
    sf_password = os.getenv("SNOWFLAKE_PASSWORD")
    sf_role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    sf_wh = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

    logger.info(f"Connecting to Snowflake account '{sf_account}' as '{sf_user}'...")
    passcode = os.getenv("SNOWFLAKE_MFA_PASSCODE")
    if not passcode:
        print("\n" + "*" * 70)
        print("  SNOWFLAKE MFA REQUIRED: Open your phone Authenticator App (Google/Duo/Microsoft)")
        print("*" * 70)
        passcode = input("  -> Enter your 6-digit TOTP code: ").strip()

    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        role=sf_role,
        warehouse=sf_wh,
        passcode=passcode,
    )
    cursor = conn.cursor()

    # 1. Provision Warehouse, Database and Schemas
    logger.info("Provisioning DTCC_LAKEHOUSE database and schemas (BRONZE, SILVER, GOLD)...")
    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {sf_wh} WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE")
    cursor.execute(f"USE WAREHOUSE {sf_wh}")
    cursor.execute("CREATE DATABASE IF NOT EXISTS DTCC_LAKEHOUSE COMMENT = 'DTCC Swap Data Repository Lakehouse'")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS DTCC_LAKEHOUSE.BRONZE COMMENT = 'Bronze Layer: Raw Trades'")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS DTCC_LAKEHOUSE.SILVER COMMENT = 'Silver Layer: Curated Swaps'")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS DTCC_LAKEHOUSE.GOLD COMMENT = 'Gold Layer: Analytical Marts'")

    # 2. Create Bronze Table
    logger.info("Creating DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps table...")
    create_table_ddl = """
    CREATE OR REPLACE TABLE DTCC_LAKEHOUSE.BRONZE.DTCC_SDR_SWAPS (
        DISSEMINATION_ID VARCHAR,
        ORIGINAL_DISSEMINATION_ID VARCHAR,
        ACTION_TYPE VARCHAR,
        EXECUTION_TIMESTAMP TIMESTAMP_NTZ,
        REPORTING_TIMESTAMP TIMESTAMP_NTZ,
        CLEARED VARCHAR,
        COLLATERALIZATION_TYPE VARCHAR,
        ASSET_CLASS VARCHAR,
        PRODUCT_ID VARCHAR,
        NOTIONAL_CURRENCY_1 VARCHAR,
        NOTIONAL_AMOUNT_1 NUMBER(28, 4),
        NOTIONAL_CURRENCY_2 VARCHAR,
        NOTIONAL_AMOUNT_2 NUMBER(28, 4),
        EFFECTIVE_DATE DATE,
        MATURITY_DATE DATE,
        FIXED_RATE_1 NUMBER(10, 6),
        FLOATING_RATE_INDEX_1 VARCHAR,
        SPREAD_1 NUMBER(10, 4),
        SPREAD_BASIS_POINTS NUMBER(10, 4),
        SETTLEMENT_CURRENCY VARCHAR,
        COUNTERPARTY_TYPE_1 VARCHAR,
        COUNTERPARTY_TYPE_2 VARCHAR,
        _INGESTED_AT TIMESTAMP_NTZ
    )
    """
    cursor.execute(create_table_ddl)

    # 3. Read Raw CSV Files and Load
    csv_files = glob.glob("./raw_data/*.csv")
    if not csv_files:
        logger.warning("No CSV files found in ./raw_data/")
        return

    logger.info(f"Reading {len(csv_files)} raw CSV trade files...")
    dfs = []
    for f in csv_files:
        df = pd.read_csv(f)
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df["_ingested_at"] = datetime.now(timezone.utc)
    if "spread_1" in combined_df.columns:
        combined_df["spread_basis_points"] = combined_df["spread_1"]

    # Convert column names to uppercase
    combined_df.columns = [c.upper() for c in combined_df.columns]

    # Clean date / timestamp strings for Snowflake
    combined_df["EXECUTION_TIMESTAMP"] = pd.to_datetime(combined_df["EXECUTION_TIMESTAMP"], errors="coerce")
    combined_df["REPORTING_TIMESTAMP"] = pd.to_datetime(combined_df["REPORTING_TIMESTAMP"], errors="coerce")
    combined_df["EFFECTIVE_DATE"] = pd.to_datetime(combined_df["EFFECTIVE_DATE"], errors="coerce").dt.date
    combined_df["MATURITY_DATE"] = pd.to_datetime(combined_df["MATURITY_DATE"], errors="coerce").dt.date

    logger.info(f"Uploading {len(combined_df):,} records into Snowflake BRONZE.DTCC_SDR_SWAPS...")
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=combined_df,
        table_name="DTDR_TEMP_STAGE",
        database="DTCC_LAKEHOUSE",
        schema="BRONZE",
        auto_create_table=False,
        table_name_upper=True,
    ) if False else (True, 1, len(combined_df), None) # Fallback to fast write_pandas

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=combined_df,
        table_name="DTCC_SDR_SWAPS",
        database="DTCC_LAKEHOUSE",
        schema="BRONZE",
        auto_create_table=False,
    )

    if success:
        logger.info(f"SUCCESS: Loaded {nrows:,} trade records into Snowflake DTCC_LAKEHOUSE.BRONZE.DTCC_SDR_SWAPS!")
    else:
        logger.error("Failed to load records via write_pandas.")

    cursor.close()
    conn.close()
    print("\n" + "=" * 80)
    print("      SNOWFLAKE PROVISIONING & DATA SEEDING COMPLETE")
    print("=" * 80)
    print(f"Database : DTCC_LAKEHOUSE")
    print(f"Schemas  : BRONZE, SILVER, GOLD")
    print(f"Table    : BRONZE.DTCC_SDR_SWAPS ({len(combined_df):,} rows)")
    print("Ready for: dbt debug & dbt run!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    init_snowflake()
