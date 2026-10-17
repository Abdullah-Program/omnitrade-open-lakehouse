-- ==============================================================================
-- DTCC LAKEHOUSE CROSS-ENGINE BENCHMARKING SUITE
-- ==============================================================================
-- Engines Evaluated: Snowflake vs AWS Athena / Spark / DuckDB
-- Objective:
--   Measure query execution time (Cold vs Warm cache), bytes scanned,
--   and compute cost efficiency over Iceberg format v2.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- QUERY 1: PARTITION PRUNING EFFICIENCY (Selective Point Lookup)
-- Expected: Engine reads ONLY the RATES partition manifest, skipping 80%+ S3 data.
-- ------------------------------------------------------------------------------
-- [BENCHMARK_Q1_PARTITION_PRUNING]
SELECT 
    dissemination_id,
    action_type,
    execution_timestamp,
    notional_currency_1,
    notional_amount_1,
    fixed_rate_1,
    spread_1
FROM DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps
WHERE asset_class = 'RATES'
  AND execution_timestamp >= '2026-10-01'
  AND execution_timestamp < '2026-11-01'
  AND cleared = 'C'
ORDER BY execution_timestamp DESC
LIMIT 500;

-- ------------------------------------------------------------------------------
-- QUERY 2: HEAVY AGGREGATION & VECTORIZED SCAN
-- Expected: Columnar scan over millions of trades, testing memory bandwidth.
-- ------------------------------------------------------------------------------
-- [BENCHMARK_Q2_HEAVY_AGGREGATION]
SELECT 
    asset_class,
    notional_currency_1 AS currency,
    cleared,
    COUNT(*) AS total_trade_count,
    ROUND(SUM(notional_amount_1) / 1e9, 4) AS gross_notional_billions,
    ROUND(AVG(notional_amount_1), 2) AS avg_trade_notional,
    ROUND(MIN(notional_amount_1), 2) AS min_notional,
    ROUND(MAX(notional_amount_1), 2) AS max_notional,
    ROUND(AVG(TRY_CAST(spread_1 AS FLOAT)), 2) AS avg_spread_bps
FROM DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps
GROUP BY 1, 2, 3
ORDER BY gross_notional_billions DESC;

-- ------------------------------------------------------------------------------
-- QUERY 3: ANALYTIC WINDOW FUNCTIONS (Market Anomaly Detection)
-- Expected: Memory-intensive window partitioning and standard deviation calculations.
-- ------------------------------------------------------------------------------
-- [BENCHMARK_Q3_ANALYTIC_WINDOW]
WITH ranked_notionals AS (
    SELECT
        dissemination_id,
        execution_timestamp,
        asset_class,
        product_id,
        notional_amount_1,
        AVG(notional_amount_1) OVER (PARTITION BY asset_class, product_id) AS avg_notional,
        STDDEV(notional_amount_1) OVER (PARTITION BY asset_class, product_id) AS stddev_notional
    FROM DTCC_LAKEHOUSE.BRONZE.dtcc_sdr_swaps
    WHERE execution_timestamp >= '2026-01-01'
)
SELECT 
    dissemination_id,
    execution_timestamp,
    asset_class,
    product_id,
    notional_amount_1,
    ROUND((notional_amount_1 - avg_notional) / NULLIF(stddev_notional, 0), 2) AS z_score
FROM ranked_notionals
WHERE ABS((notional_amount_1 - avg_notional) / NULLIF(stddev_notional, 0)) >= 3.0
ORDER BY z_score DESC
LIMIT 100;
