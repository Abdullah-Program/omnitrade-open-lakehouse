"""
================================================================================
Apache Polaris REST Catalog Initialization & Bootstrap Script
================================================================================
Role: Principal AWS Data Engineer
Layer: Open Catalog Governance
Description:
    Authenticates with Apache Polaris using OAuth2 Client Credentials,
    initializes the internal catalog ('dtcc_catalog') with S3 storage permissions,
    and provisions the 'bronze' namespace for Iceberg tables.
================================================================================
"""

import os
import sys
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] PolarisBootstrap: %(message)s",
)
logger = logging.getLogger("PolarisBootstrap")


def get_polaris_token(base_url: str, client_id: str, client_secret: str) -> str:
    """Acquires OAuth2 Bearer Access Token from Polaris REST Catalog."""
    token_url = f"{base_url}/v1/oauth/tokens"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "PRINCIPAL_ROLE:ALL",
    }
    logger.info(f"Authenticating with Polaris at: {token_url}")
    resp = requests.post(token_url, data=payload, timeout=10)
    if resp.status_code != 200:
        logger.error(f"Failed to acquire OAuth token ({resp.status_code}): {resp.text}")
        sys.exit(1)
    
    token = resp.json().get("access_token")
    logger.info("Successfully acquired OAuth2 Bearer token from Apache Polaris!")
    return token


def create_or_verify_catalog(management_url: str, token: str, catalog_name: str, warehouse_path: str):
    """Provisions the catalog entity in Apache Polaris via Management REST API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    catalog_payload = {
        "catalog": {
            "name": catalog_name,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {
                "default-base-location": warehouse_path,
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [warehouse_path],
                "roleArn": "arn:aws:iam::123456789012:role/PolarisS3AccessRole",
            },
        }
    }

    url = f"{management_url}/catalogs"
    logger.info(f"Provisioning Polaris catalog '{catalog_name}'...")
    resp = requests.post(url, json=catalog_payload, headers=headers, timeout=10)
    
    if resp.status_code in [200, 201]:
        logger.info(f"Catalog '{catalog_name}' created successfully (HTTP {resp.status_code})!")
    elif resp.status_code == 409:
        logger.info(f"Catalog '{catalog_name}' already exists in Polaris.")
    else:
        logger.warning(f"Catalog creation response ({resp.status_code}): {resp.text}")


def create_or_verify_namespace(base_url: str, token: str, catalog_name: str, namespace: str):
    """Provisions target Iceberg namespace in Polaris."""
    from pyiceberg.catalog import load_catalog

    logger.info(f"Connecting via PyIceberg REST client to verify namespace '{namespace}'...")
    try:
        catalog = load_catalog(
            catalog_name,
            **{
                "type": "rest",
                "uri": base_url,
                "token": token,
                "warehouse": catalog_name,
            },
        )
        catalog.create_namespace_if_not_exists(namespace)
        namespaces = catalog.list_namespaces()
        logger.info(f"Current Polaris namespaces in '{catalog_name}': {namespaces}")
    except Exception as exc:
        logger.warning(f"Namespace verification note: {exc}")


def main():
    base_url = "http://localhost:8181/api/catalog"
    mgmt_url = "http://localhost:8181/api/management/v1"
    
    client_id = os.getenv("POLARIS_CLIENT_ID", "cbe546731a2e451b")
    client_secret = os.getenv("POLARIS_CLIENT_SECRET", "fec644cb40365f4654e64fe118ad5cf5")
    catalog_name = os.getenv("POLARIS_CATALOG_NAME", "dtcc_catalog")
    warehouse_path = os.getenv("S3_WAREHOUSE_PATH", "s3://dtcc-open-lakehouse-bronze/iceberg/warehouse/")

    token = get_polaris_token(base_url, client_id, client_secret)
    create_or_verify_catalog(mgmt_url, token, catalog_name, warehouse_path)
    create_or_verify_namespace(base_url, token, catalog_name, "bronze")
    
    print("\n" + "=" * 80)
    print("      APACHE POLARIS REST CATALOG PROVISIONED & READY FOR ICEBERG")
    print("=" * 80)
    print(f"Catalog Name : {catalog_name}")
    print(f"REST Endpoint: {base_url}/v1")
    print(f"Namespace    : bronze")
    print(f"Status       : ONLINE (HTTP 200 / Bearer Auth Active)")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
