"""
================================================================================
Apache Polaris REST Catalog Registration Script
================================================================================
Role: Principal AWS Data Engineer
Layer: Catalog Management & Governance
Description:
    Registers or updates Iceberg table metadata locations directly with the
    Apache Polaris REST Catalog. Enables Snowflake, Trino, Athena, and DuckDB
    to immediately discover and query the table without metadata copying.
================================================================================
"""

import os
import sys
import json
import logging
import argparse
from typing import Optional, Dict, Any
import requests
from dotenv import load_dotenv

# PyIceberg client
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

# Load environment configuration
load_dotenv()

# Setup logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] PolarisRegistration: %(message)s",
)
logger = logging.getLogger("PolarisRegistration")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register Iceberg Table in Apache Polaris")
    parser.add_argument(
        "--namespace",
        type=str,
        default="bronze",
        help="Target Polaris namespace (e.g. bronze, silver, gold).",
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="dtcc_sdr_swaps",
        help="Name of the Iceberg table to register.",
    )
    parser.add_argument(
        "--metadata-location",
        type=str,
        default=None,
        help="Explicit S3 URI to the metadata JSON file (e.g. s3://bucket/iceberg/.../metadata/00000-xxx.metadata.json)",
    )
    return parser.parse_args()


def get_polaris_token(base_url: str, client_id: str, client_secret: str) -> Optional[str]:
    """Obtains OAuth2 bearer token from Apache Polaris REST catalog."""
    token_url = f"{base_url}/oauth/tokens"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "PRINCIPAL_ROLE:ALL",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        logger.info(f"Requesting OAuth2 access token from Polaris: {token_url}")
        resp = requests.post(token_url, data=payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            token = resp.json().get("access_token")
            logger.info("Successfully acquired Polaris bearer token.")
            return token
        else:
            logger.warning(f"Polaris auth failed with code {resp.status_code}: {resp.text}")
            return None
    except requests.RequestException as exc:
        logger.warning(f"Could not reach Polaris OAuth endpoint: {exc}")
        return None


def register_table_via_rest(
    base_url: str,
    catalog_name: str,
    namespace: str,
    table_name: str,
    metadata_location: str,
    token: Optional[str] = None,
) -> bool:
    """
    Registers an Iceberg table using the official Iceberg REST Catalog Register Table API.
    POST /v1/{prefix}/namespaces/{namespace}/register
    """
    url = f"{base_url}/{catalog_name}/namespaces/{namespace}/register"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    body = {
        "name": table_name,
        "metadata-location": metadata_location,
    }

    logger.info(f"Registering table in Polaris: {namespace}.{table_name} -> {metadata_location}")
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=15)
        if resp.status_code in [200, 204]:
            logger.info(f"Successfully registered '{table_name}' in Polaris REST catalog!")
            return True
        elif resp.status_code == 409:
            logger.warning(f"Table '{table_name}' already exists in namespace '{namespace}'.")
            return True
        else:
            logger.error(f"Polaris registration failed ({resp.status_code}): {resp.text}")
            return False
    except requests.RequestException as exc:
        logger.error(f"HTTP request error during table registration: {exc}")
        return False


def verify_table_via_pyiceberg(
    catalog_name: str,
    polaris_url: str,
    namespace: str,
    table_name: str,
):
    """
    Verifies that PyIceberg can read and resolve the registered table schema and snapshots.
    """
    logger.info(f"Connecting to Polaris catalog via PyIceberg client...")
    client_id = os.getenv("POLARIS_CLIENT_ID", "polaris-admin")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "polaris-secret")
    s3_warehouse = os.getenv("S3_WAREHOUSE_PATH", "s3://dtcc-open-lakehouse-bronze/iceberg/warehouse/")

    try:
        catalog = load_catalog(
            catalog_name,
            **{
                "type": "rest",
                "uri": polaris_url,
                "credential": f"{client_id}:{client_secret}",
                "warehouse": s3_warehouse,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )

        table_fqn = f"{namespace}.{table_name}"
        logger.info(f"Loading table metadata for: {table_fqn}")
        table = catalog.load_table(table_fqn)

        logger.info(f"Table loaded successfully!")
        logger.info(f"  - Current Snapshot ID: {table.current_snapshot().snapshot_id if table.current_snapshot() else 'None'}")
        logger.info(f"  - Schema Columns: {[col.name for col in table.schema().columns]}")
        logger.info(f"  - Partition Spec: {table.spec()}")
        logger.info(f"  - Metadata Location: {table.metadata_location}")
        return True
    except Exception as exc:
        logger.warning(f"PyIceberg verification returned: {exc}. (Expected if table has not had initial commit)")
        return False


def main():
    args = parse_arguments()
    base_url = os.getenv("POLARIS_BASE_URL", "http://localhost:8181/api/catalog/v1")
    catalog_name = os.getenv("POLARIS_CATALOG_NAME", "dtcc_catalog")
    client_id = os.getenv("POLARIS_CLIENT_ID", "polaris-admin")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "polaris-secret")

    # If explicit metadata location not passed, resolve standard S3 metadata path
    metadata_loc = args.metadata_location
    if not metadata_loc:
        s3_warehouse = os.getenv("S3_WAREHOUSE_PATH", "s3://dtcc-open-lakehouse-bronze/iceberg/warehouse/")
        metadata_loc = f"{s3_warehouse.rstrip('/')}/{args.namespace}/{args.table_name}/metadata/latest.metadata.json"

    token = get_polaris_token(base_url, client_id, client_secret)
    register_table_via_rest(base_url, catalog_name, args.namespace, args.table_name, metadata_loc, token)
    verify_table_via_pyiceberg(catalog_name, base_url, args.namespace, args.table_name)


if __name__ == "__main__":
    main()
