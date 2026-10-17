{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'marts', 'reporting']
  )
}}

/*
================================================================================
Model: fct_daily_settlement_volume
Layer: Gold (Analytics Mart)
Description:
    Daily aggregated cleared vs uncleared derivatives volume, notional totals in USD,
    clearing penetration ratios, and weighted execution pricing (spread/fixed rate).
================================================================================
*/

WITH enriched_trades AS (
    SELECT * FROM {{ ref('int_trades_enriched') }}
),

daily_aggregation AS (
    SELECT
        CAST(execution_timestamp AS DATE) AS trade_date,
        asset_class,
        notional_currency_1 AS currency,
        COUNT(trade_lineage_id) AS total_trades,
        COUNT(CASE WHEN is_cleared THEN 1 END) AS cleared_trades,
        COUNT(CASE WHEN NOT is_cleared THEN 1 END) AS uncleared_trades,
        ROUND(
            COUNT(CASE WHEN is_cleared THEN 1 END) * 100.0 / NULLIF(COUNT(trade_lineage_id), 0),
            2
        ) AS clearing_rate_percentage,

        -- Notional Volume Metrics (USD)
        ROUND(SUM(notional_amount_usd), 2) AS total_notional_usd,
        ROUND(SUM(CASE WHEN is_cleared THEN notional_amount_usd ELSE 0 END), 2) AS cleared_notional_usd,
        ROUND(SUM(CASE WHEN NOT is_cleared THEN notional_amount_usd ELSE 0 END), 2) AS uncleared_notional_usd,

        -- Trade Size Statistics
        ROUND(AVG(notional_amount_usd), 2) AS avg_trade_size_usd,
        ROUND(MAX(notional_amount_usd), 2) AS max_trade_size_usd,
        ROUND(AVG(tenor_years), 2) AS avg_tenor_years,

        -- Volume-Weighted Execution Metrics
        ROUND(
            SUM(COALESCE(spread_1, 0) * notional_amount_usd) /
            NULLIF(SUM(CASE WHEN spread_1 IS NOT NULL THEN notional_amount_usd ELSE 0 END), 0),
            4
        ) AS weighted_avg_spread_bps,

        ROUND(
            SUM(COALESCE(fixed_rate_1, 0) * notional_amount_usd) /
            NULLIF(SUM(CASE WHEN fixed_rate_1 IS NOT NULL THEN notional_amount_usd ELSE 0 END), 0),
            6
        ) AS weighted_avg_fixed_rate

    FROM enriched_trades
    GROUP BY
        CAST(execution_timestamp AS DATE),
        asset_class,
        notional_currency_1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['trade_date', 'asset_class', 'currency']) }} AS settlement_volume_key,
    trade_date,
    asset_class,
    currency,
    total_trades,
    cleared_trades,
    uncleared_trades,
    clearing_rate_percentage,
    total_notional_usd,
    cleared_notional_usd,
    uncleared_notional_usd,
    avg_trade_size_usd,
    max_trade_size_usd,
    avg_tenor_years,
    weighted_avg_spread_bps,
    weighted_avg_fixed_rate,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM daily_aggregation
