{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'marts', 'surveillance', 'anomaly_detection']
  )
}}

/*
================================================================================
Model: fct_risk_anomaly_flags
Layer: Gold (Market Surveillance & Statistical Anomaly Detection)
Description:
    Applies statistical z-score modeling to identify fat-finger errors,
    block trades, and sudden liquidity spikes across SDR asset classes.
    Flag threshold: Absolute Z-Score >= 3.0 (99.7% confidence interval).
================================================================================
*/

WITH enriched_trades AS (
    SELECT * FROM {{ ref('int_trades_enriched') }}
),

cohort_statistics AS (
    SELECT
        trade_lineage_id,
        dissemination_id,
        execution_timestamp,
        asset_class,
        product_id,
        notional_currency_1,
        notional_amount_usd,
        tenor_years,
        spread_1,
        clearing_status,
        counterparty_type_1,
        counterparty_type_2,

        -- Cohort Moving Statistics partitioned by Asset Class & Product
        AVG(notional_amount_usd) OVER (
            PARTITION BY asset_class, product_id
        ) AS cohort_avg_notional_usd,

        STDDEV(notional_amount_usd) OVER (
            PARTITION BY asset_class, product_id
        ) AS cohort_stddev_notional_usd

    FROM enriched_trades
),

anomaly_scoring AS (
    SELECT
        trade_lineage_id,
        dissemination_id,
        execution_timestamp,
        asset_class,
        product_id,
        notional_currency_1,
        notional_amount_usd,
        cohort_avg_notional_usd,
        cohort_stddev_notional_usd,
        tenor_years,
        spread_1,
        clearing_status,
        counterparty_type_1,
        counterparty_type_2,

        -- Statistical Z-Score Calculation
        CASE
            WHEN COALESCE(cohort_stddev_notional_usd, 0) > 0
            THEN ROUND((notional_amount_usd - cohort_avg_notional_usd) / cohort_stddev_notional_usd, 3)
            ELSE 0
        END AS notional_z_score

    FROM cohort_statistics
),

flagged_anomalies AS (
    SELECT
        *,
        -- Anomaly Categorization & Severity Scoring
        CASE
            WHEN notional_z_score >= 4.0 THEN 'CRITICAL'
            WHEN notional_z_score >= 3.0 THEN 'HIGH'
            WHEN notional_z_score >= 2.0 THEN 'MEDIUM'
            ELSE 'NORMAL'
        END AS anomaly_severity,

        CASE
            WHEN notional_z_score >= 3.0 THEN TRUE
            ELSE FALSE
        END AS is_notional_spike_flag,

        CASE
            WHEN tenor_years > 30.0 THEN TRUE
            ELSE FALSE
        END AS is_ultra_long_tenor_flag

    FROM anomaly_scoring
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['dissemination_id', 'execution_timestamp']) }} AS anomaly_surrogate_key,
    dissemination_id,
    trade_lineage_id,
    execution_timestamp,
    asset_class,
    product_id,
    notional_currency_1,
    notional_amount_usd,
    cohort_avg_notional_usd,
    cohort_stddev_notional_usd,
    notional_z_score,
    anomaly_severity,
    is_notional_spike_flag,
    is_ultra_long_tenor_flag,
    tenor_years,
    spread_1,
    clearing_status,
    counterparty_type_1,
    counterparty_type_2,
    CURRENT_TIMESTAMP() AS detected_at
FROM flagged_anomalies
-- Surface items requiring risk or surveillance review
WHERE is_notional_spike_flag = TRUE 
   OR is_ultra_long_tenor_flag = TRUE
