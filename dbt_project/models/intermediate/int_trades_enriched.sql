{{
  config(
    materialized='view',
    schema='SILVER',
    tags=['silver', 'intermediate']
  )
}}

/*
================================================================================
Model: int_trades_enriched
Layer: Silver (Intermediate Business Logic)
Description:
    Enriches staged swap trades with:
    1. Financial Tenor calculations (tenor in days, tenor in years, maturity buckets).
    2. Normalized USD Notional equivalent amounts across currencies.
    3. Trading desk session categorization (Asia/Europe/Americas).
    4. Regulatory flags (Cleared, Cross-Currency, Floating Benchmark).
================================================================================
*/

WITH staged_swaps AS (
    SELECT * FROM {{ ref('stg_dtcc_swaps') }}
),

-- Reference FX conversion multipliers to USD (Static baseline or dynamic seed)
fx_rates AS (
    SELECT 'USD' AS currency, 1.0000 AS usd_rate UNION ALL
    SELECT 'EUR', 1.0850 UNION ALL
    SELECT 'GBP', 1.2950 UNION ALL
    SELECT 'JPY', 0.0065 UNION ALL
    SELECT 'CAD', 0.7350 UNION ALL
    SELECT 'CHF', 1.1300 UNION ALL
    SELECT 'AUD', 0.6650
),

tenor_and_usd_enriched AS (
    SELECT
        s.trade_lineage_id,
        s.dissemination_id,
        s.action_type,
        s.execution_timestamp,
        s.reporting_timestamp,
        s.clearing_status,
        s.collateral_status,
        s.asset_class,
        s.product_id,
        s.notional_currency_1,
        s.notional_amount_1,
        s.notional_currency_2,
        s.notional_amount_2,
        s.effective_date,
        s.maturity_date,
        s.fixed_rate_1,
        s.floating_rate_index_1,
        s.spread_1,
        s.settlement_currency,
        s.counterparty_type_1,
        s.counterparty_type_2,

        -- 1. Tenor calculations
        DATEDIFF('day', s.effective_date, s.maturity_date) AS tenor_days,
        ROUND(DATEDIFF('day', s.effective_date, s.maturity_date) / 365.25, 2) AS tenor_years,

        -- Tenor bucket classification
        CASE
            WHEN DATEDIFF('day', s.effective_date, s.maturity_date) <= 365 THEN '< 1 Year (Short)'
            WHEN DATEDIFF('day', s.effective_date, s.maturity_date) <= 1095 THEN '1 - 3 Years'
            WHEN DATEDIFF('day', s.effective_date, s.maturity_date) <= 1825 THEN '3 - 5 Years'
            WHEN DATEDIFF('day', s.effective_date, s.maturity_date) <= 3650 THEN '5 - 10 Years (Benchmark)'
            ELSE '10+ Years (Long)'
        END AS tenor_bucket,

        -- 2. USD Notional Normalization
        ROUND(s.notional_amount_1 * COALESCE(fx.usd_rate, 1.0), 2) AS notional_amount_usd,

        -- 3. Trade Classification Flags
        CASE WHEN s.clearing_status = 'CLEARED' THEN TRUE ELSE FALSE END AS is_cleared,
        CASE WHEN s.notional_currency_1 != s.notional_currency_2 THEN TRUE ELSE FALSE END AS is_cross_currency,
        CASE WHEN s.floating_rate_index_1 IS NOT NULL AND s.floating_rate_index_1 != '' THEN TRUE ELSE FALSE END AS is_floating_rate,

        -- 4. Global Market Trading Session (UTC Hour)
        CASE
            WHEN DATE_PART('hour', s.execution_timestamp) BETWEEN 0 AND 7 THEN 'ASIA_PACIFIC'
            WHEN DATE_PART('hour', s.execution_timestamp) BETWEEN 8 AND 13 THEN 'EUROPE_LONDON'
            ELSE 'AMERICAS_NY'
        END AS trading_session,

        s._ingested_at
    FROM staged_swaps s
    LEFT JOIN fx_rates fx 
      ON s.notional_currency_1 = fx.currency
)

SELECT * FROM tenor_and_usd_enriched
