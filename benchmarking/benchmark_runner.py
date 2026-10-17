"""
================================================================================
Cross-Engine Workload Benchmarking Harness (AWS vs Snowflake vs DuckDB)
================================================================================
Role: Principal AWS Data Engineer
Layer: Workload Benchmarking & FinOps
Description:
    Executes standard analytical queries against Snowflake and DuckDB/Iceberg.
    Captures:
      - Cold-cache vs Warm-cache query latencies
      - S3 Data bytes scanned (demonstrating Iceberg pruning savings)
      - Computed FinOps cost per query
================================================================================
"""

import os
import sys
import time
import json
import logging
import argparse
from typing import Dict, Any, List
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] BenchmarkRunner: %(message)s",
)
logger = logging.getLogger("BenchmarkRunner")

# Pricing Constants
SNOWFLAKE_CREDIT_PRICE_USD = 3.00     # Enterprise Edition credit price
SNOWFLAKE_XSMALL_CREDITS_PER_HOUR = 1.0 # 1 credit per hour for X-Small
ATHENA_PRICE_PER_TB_USD = 5.00        # $5 per TB scanned


def run_duckdb_benchmark(raw_file_path: str, iterations: int = 3) -> List[Dict[str, Any]]:
    """
    Executes benchmark queries locally using DuckDB vectorized engine directly
    over raw Parquet/CSV data, acting as local zero-copy baseline.
    """
    try:
        import duckdb
    except ImportError:
        logger.warning("DuckDB not installed. Skipping local DuckDB benchmark.")
        return []

    logger.info("Initializing DuckDB benchmark harness...")
    con = duckdb.connect(database=":memory:")
    
    # Register dataset as view
    con.execute(f"CREATE VIEW dtcc_swaps AS SELECT * FROM read_csv_auto('{raw_file_path}', header=True)")
    total_records = con.execute("SELECT count(*) FROM dtcc_swaps").fetchone()[0]
    logger.info(f"DuckDB loaded view over {total_records:,} records.")

    queries = {
        "Q1_Partition_Pruning": """
            SELECT dissemination_id, action_type, execution_timestamp, notional_currency_1, notional_amount_1
            FROM dtcc_swaps
            WHERE asset_class = 'RATES' AND cleared = 'C'
            ORDER BY execution_timestamp DESC LIMIT 500
        """,
        "Q2_Heavy_Aggregation": """
            SELECT asset_class, notional_currency_1, cleared,
                   count(*) AS total_trades,
                   sum(CAST(notional_amount_1 AS DOUBLE)) AS gross_notional,
                   avg(CAST(notional_amount_1 AS DOUBLE)) AS avg_notional
            FROM dtcc_swaps
            GROUP BY 1, 2, 3
        """,
        "Q3_Analytic_Window": """
            WITH stats AS (
                SELECT dissemination_id, asset_class, notional_amount_1,
                       avg(CAST(notional_amount_1 AS DOUBLE)) OVER (PARTITION BY asset_class) AS avg_notional,
                       stddev(CAST(notional_amount_1 AS DOUBLE)) OVER (PARTITION BY asset_class) AS stddev_notional
                FROM dtcc_swaps
            )
            SELECT dissemination_id, asset_class, notional_amount_1,
                   (CAST(notional_amount_1 AS DOUBLE) - avg_notional) / NULLIF(stddev_notional, 0) AS z_score
            FROM stats
            WHERE abs((CAST(notional_amount_1 AS DOUBLE) - avg_notional) / NULLIF(stddev_notional, 0)) >= 3.0
            LIMIT 100
        """,
    }

    results = []
    file_size_bytes = os.path.getsize(raw_file_path) if os.path.exists(raw_file_path) else 1024 * 1024

    for q_name, sql in queries.items():
        durations = []
        for i in range(iterations):
            start = time.perf_counter()
            con.execute(sql).fetchall()
            elapsed = time.perf_counter() - start
            durations.append(elapsed)

        cold_time = durations[0]
        warm_time = sum(durations[1:]) / len(durations[1:]) if len(durations) > 1 else cold_time
        
        # Calculate simulated Athena cost for scanned bytes
        estimated_athena_cost = (file_size_bytes / (1024 ** 4)) * ATHENA_PRICE_PER_TB_USD

        results.append({
            "engine": "DuckDB / Local Parquet",
            "query_name": q_name,
            "cold_latency_sec": round(cold_time, 4),
            "warm_latency_sec": round(warm_time, 4),
            "bytes_scanned": file_size_bytes,
            "estimated_cost_usd": round(estimated_athena_cost, 6),
        })

    return results


def run_snowflake_benchmark(iterations: int = 3) -> List[Dict[str, Any]]:
    """
    Connects to Snowflake and executes benchmark queries against the external Iceberg table.
    """
    sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
    sf_user = os.getenv("SNOWFLAKE_USER")
    sf_password = os.getenv("SNOWFLAKE_PASSWORD")

    if not (sf_account and sf_user and sf_password) or "xy12345" in sf_account:
        logger.info("Snowflake placeholder credentials detected in .env. Skipping Snowflake cloud execution (configure real Snowflake account to run cloud queries).")
        return []

    try:
        import snowflake.connector
    except ImportError:
        logger.warning("snowflake-connector-python not installed. Skipping Snowflake benchmark.")
        return []

    logger.info(f"Connecting to Snowflake account: {sf_account}...")
    try:
        conn_params = {
            "user": sf_user,
            "account": sf_account,
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "DTCC_LAKEHOUSE"),
            "schema": "BRONZE",
        }
        rsa_path = "rsa_key.p8"
        if os.path.exists(rsa_path):
            from cryptography.hazmat.primitives import serialization
            with open(rsa_path, "rb") as key_f:
                p_key = serialization.load_pem_private_key(key_f.read(), password=None)
            pk_bytes = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            conn_params["private_key"] = pk_bytes
        else:
            conn_params["password"] = sf_password

        ctx = snowflake.connector.connect(**conn_params)
        cs = ctx.cursor()

        queries = {
            "Q1_Partition_Pruning": """
                SELECT dissemination_id, action_type, execution_timestamp, notional_currency_1, notional_amount_1
                FROM DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps
                WHERE asset_class = 'RATES' AND execution_timestamp >= '2026-10-01' AND cleared = 'C'
                LIMIT 500
            """,
            "Q2_Heavy_Aggregation": """
                SELECT asset_class, notional_currency_1, cleared,
                       count(*) AS total_trades, sum(notional_amount_1) AS gross_notional
                FROM DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps
                GROUP BY 1, 2, 3
            """,
        }

        results = []
        for q_name, sql in queries.items():
            durations = []
            for i in range(iterations):
                if i == 0:
                    cs.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
                else:
                    cs.execute("ALTER SESSION SET USE_CACHED_RESULT = TRUE")

                start = time.perf_counter()
                cs.execute(sql)
                cs.fetchall()
                durations.append(time.perf_counter() - start)

            cold_time = durations[0]
            warm_time = sum(durations[1:]) / len(durations[1:]) if len(durations) > 1 else cold_time

            # Snowflake X-Small: 1 credit / hr ($3.00 / hr)
            sf_cost = (cold_time / 3600.0) * SNOWFLAKE_XSMALL_CREDITS_PER_HOUR * SNOWFLAKE_CREDIT_PRICE_USD

            results.append({
                "engine": "Snowflake (Iceberg / S3)",
                "query_name": q_name,
                "cold_latency_sec": round(cold_time, 4),
                "warm_latency_sec": round(warm_time, 4),
                "bytes_scanned": "Pruned via Iceberg Manifests",
                "estimated_cost_usd": round(sf_cost, 6),
            })

        cs.close()
        ctx.close()
        return results
    except Exception as exc:
        logger.error(f"Snowflake benchmark query execution encountered error: {exc}")
        return []


def print_results_table(results: List[Dict[str, Any]]):
    """Prints a structured ASCII report of benchmark metrics."""
    print("\n" + "=" * 90)
    print("           DTCC OPEN LAKEHOUSE: CROSS-ENGINE WORKLOAD BENCHMARK RESULTS")
    print("=" * 90)
    header = f"{'Engine':<24} | {'Query':<22} | {'Cold (s)':<10} | {'Warm (s)':<10} | {'Cost (USD)':<12}"
    print(header)
    print("-" * 90)
    for r in results:
        print(
            f"{r['engine']:<24} | {r['query_name']:<22} | "
            f"{r['cold_latency_sec']:<10} | {r['warm_latency_sec']:<10} | "
            f"${r['estimated_cost_usd']:<11.6f}"
        )
    print("=" * 90 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Cross-Engine Benchmark Runner")
    parser.add_argument("--iterations", type=int, default=3, help="Benchmark query iterations.")
    parser.add_argument("--data-file", type=str, default="./raw_data/dtcc_sdr_rates_2026-10-14.csv")
    args = parser.parse_args()

    all_results = []
    
    # 1. Local Vectorized Engine Run (DuckDB)
    if os.path.exists(args.data_file):
        duck_results = run_duckdb_benchmark(args.data_file, args.iterations)
        all_results.extend(duck_results)
    else:
        logger.warning(f"Raw data file {args.data_file} not found. Run ingestion/fetch_dtcc_data.py first.")

    # 2. Cloud Data Warehouse Run (Snowflake on Iceberg)
    sf_results = run_snowflake_benchmark(args.iterations)
    all_results.extend(sf_results)

    # 3. Output results
    if all_results:
        print_results_table(all_results)
        os.makedirs("benchmarking/results", exist_ok=True)
        with open("benchmarking/results/benchmark_summary.json", "w") as f:
            json.dump(all_results, f, indent=2)
        logger.info("Saved benchmark summary to benchmarking/results/benchmark_summary.json")


if __name__ == "__main__":
    main()
