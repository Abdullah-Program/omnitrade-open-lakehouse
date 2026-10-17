{{
  config(
    materialized='dynamic_table',
    snowflake_warehouse='COMPUTE_WH',
    target_lag='1 minute',
    schema='GOLD',
    tags=['gold', 'dynamic_table', 'streaming', 'realtime']
  )
}}

/*
================================================================================
Model: dynamic_live_market_surveillance
Layer: Gold (Snowflake Continuous Streaming Dynamic Table)
Target Lag: 1 minute
Description:
    Demonstrates Snowflake's declarative continuous processing over Iceberg/Bronze.
    Automatically refreshes downstream risk exposure & volume metrics every 1 minute
    without requiring external scheduling or Airflow/cron jobs.
================================================================================
*/

SELECT 
    asset_class,
    notional_currency_1 AS currency,
    COUNT(*) AS active_trades,
    COUNT(CASE WHEN is_cleared THEN 1 END) AS cleared_trades,
    ROUND(SUM(notional_amount_usd) / 1e6, 2) AS total_notional_millions_usd,
    ROUND(AVG(notional_amount_usd) / 1e3, 2) AS avg_trade_size_thousands_usd,
    MAX(execution_timestamp) AS latest_trade_executed_at
FROM {{ ref('int_trades_enriched') }}
GROUP BY 1, 2
