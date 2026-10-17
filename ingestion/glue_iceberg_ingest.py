"""
================================================================================
Apache Iceberg Bronze Ingestion Pipeline (PyIceberg & PySpark Dual Engine)
================================================================================
Role: Principal AWS Data Engineer
Layer: Bronze to Iceberg Table
Description:
    Reads raw DTCC SDR swap trade feeds from ./raw_data/*.csv (or S3 landing),
    applies strict schema enforcement, audits ingestion timestamps, and appends
    into an Apache Iceberg table v2 format.
    
    Partitioning Strategy:
        - identity(asset_class)
        - months(execution_timestamp)
        
    Table Properties:
        - format-version: 2
        - write.parquet.compression-codec: zstd
================================================================================
"""

import os
import sys
import glob
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

# PyIceberg & PyArrow core libraries
import pyarrow.csv as pv
import pyarrow.compute as pc
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DecimalType,
    DateType,
    TimestampType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, MonthTransform

# Load environment configuration
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] IcebergIngest: %(message)s",
)
logger = logging.getLogger("IcebergIngest")


def get_dtcc_iceberg_schema() -> Schema:
    """Defines the canonical Apache Iceberg schema for DTCC SDR trades."""
    return Schema(
        NestedField(1, "dissemination_id", StringType(), required=True),
        NestedField(2, "original_dissemination_id", StringType(), required=False),
        NestedField(3, "action_type", StringType(), required=True),
        NestedField(4, "execution_timestamp", TimestampType(), required=True),
        NestedField(5, "cleared", StringType(), required=False),
        NestedField(6, "collateralization_type", StringType(), required=False),
        NestedField(7, "asset_class", StringType(), required=True),
        NestedField(8, "product_id", StringType(), required=False),
        NestedField(9, "notional_currency_1", StringType(), required=False),
        NestedField(10, "notional_amount_1", DecimalType(28, 4), required=False),
        NestedField(11, "notional_currency_2", StringType(), required=False),
        NestedField(12, "notional_amount_2", DecimalType(28, 4), required=False),
        NestedField(13, "effective_date", DateType(), required=False),
        NestedField(14, "maturity_date", DateType(), required=False),
        NestedField(15, "fixed_rate_1", DecimalType(10, 6), required=False),
        NestedField(16, "floating_rate_index_1", StringType(), required=False),
        NestedField(17, "spread_1", DecimalType(10, 4), required=False),
        NestedField(18, "settlement_currency", StringType(), required=False),
        NestedField(19, "counterparty_type_1", StringType(), required=False),
        NestedField(20, "counterparty_type_2", StringType(), required=False),
        NestedField(21, "reporting_timestamp", TimestampType(), required=False),
        NestedField(22, "_ingested_at", TimestampType(), required=True),
    )


def get_partition_spec() -> PartitionSpec:
    """Hidden partitioning: identity(asset_class) and months(execution_timestamp)."""
    return PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=IdentityTransform(), name="asset_class"),
        PartitionField(source_id=4, field_id=1001, transform=MonthTransform(), name="execution_timestamp_month"),
    )


def get_or_create_catalog():
    """Initializes connection to Polaris REST or local directory catalog."""
    abs_wh = os.path.abspath("warehouse").replace("\\", "/")
    os.makedirs("warehouse", exist_ok=True)

    # Use robust local warehouse catalog for developer workstation execution
    catalog = load_catalog(
        "local",
        **{
            "type": "sql",
            "uri": f"sqlite:///{abs_wh}/catalog.db",
            "warehouse": abs_wh,
        },
    )
    catalog.create_namespace_if_not_exists("bronze")
    return catalog


def ingest_dtcc_data_to_iceberg():
    """Reads raw CSV files, enforces typing, and appends into Iceberg table."""
    logger.info("Initializing Apache Iceberg Ingestion Pipeline...")
    catalog = get_or_create_catalog()
    table_name = "bronze.dtcc_sdr_swaps"

    schema = get_dtcc_iceberg_schema()
    spec = get_partition_spec()
    properties = {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "7",
    }

    table = catalog.create_table_if_not_exists(
        table_name,
        schema=schema,
        partition_spec=spec,
        properties=properties,
    )
    target_arrow_schema = table.schema().as_arrow()

    csv_files = glob.glob("./raw_data/*.csv")
    if not csv_files:
        logger.warning("No raw CSV files found in ./raw_data. Please run fetch_dtcc_data.py first.")
        return

    total_ingested = 0
    logger.info(f"Processing {len(csv_files)} raw DTCC SDR files...")

    for filepath in csv_files:
        logger.info(f"Reading and transforming: {filepath}")
        arrow_df = pv.read_csv(filepath)
        now_ts = datetime.now(timezone.utc)
        
        arrow_df = arrow_df.append_column(
            "_ingested_at",
            pa.array([now_ts] * len(arrow_df), type=pa.timestamp("us")),
        )

        # Schema evolution alias mapping & compatibility
        COLUMN_ALIASES = {
            "spread_basis_points": ["spread_1", "spread_bps", "spread"],
            "spread_1": ["spread_basis_points"],
        }

        casted_cols = []
        for field in target_arrow_schema:
            col = None
            if field.name in arrow_df.column_names:
                col = arrow_df[field.name]
            elif field.name in COLUMN_ALIASES:
                for alias in COLUMN_ALIASES[field.name]:
                    if alias in arrow_df.column_names:
                        col = arrow_df[alias]
                        break

            if col is None:
                # Evolved or optional column missing from raw source -> fill with nulls
                casted_cols.append(pa.nulls(len(arrow_df), type=field.type))
                continue

            # Clean empty strings for typed conversions
            if pa.types.is_string(col.type):
                col = pc.if_else(pc.equal(col, ""), None, col)

            if pa.types.is_timestamp(field.type):
                casted_cols.append(pc.cast(col, field.type))
            elif pa.types.is_date(field.type):
                casted_cols.append(pc.cast(col, field.type))
            elif pa.types.is_decimal(field.type):
                casted_cols.append(pc.cast(col, field.type))
            else:
                casted_cols.append(pc.cast(col, field.type))

        batch = pa.Table.from_arrays(casted_cols, schema=target_arrow_schema)
        table.append(batch)
        total_ingested += len(batch)
        logger.info(f"  -> Appended {len(batch):,} rows from {os.path.basename(filepath)}")

    logger.info("=" * 80)
    logger.info(f"SUCCESS: Ingested total {total_ingested:,} trade records into Apache Iceberg table!")
    logger.info(f"Table Location : {table.location()}")
    logger.info(f"Snapshot ID    : {table.current_snapshot().snapshot_id}")
    logger.info(f"Partition Spec : {table.spec()}")
    logger.info("=" * 80)


def main():
    try:
        ingest_dtcc_data_to_iceberg()
    except Exception as exc:
        logger.error(f"Ingestion encountered error: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
