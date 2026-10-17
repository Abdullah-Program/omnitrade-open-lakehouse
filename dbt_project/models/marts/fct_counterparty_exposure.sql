{{
  config(
    materialized='table',
    schema='GOLD',
    tags=['gold', 'marts', 'risk']
  )
}}

/*
================================================================================
Model: fct_counterparty_exposure
Layer: Gold (Risk & Regulatory Mart)
Description:
    Computes bilateral counterparty concentration risk and collateralization distribution.
    Identifies uncollateralized bilateral exposure across dealer, bank, client, and fund segments.
================================================================================
*/

WITH enriched_trades AS (
    SELECT * FROM {{ ref('int_trades_enriched') }}
),

exposure_aggregation AS (
    SELECT
        asset_class,
        counterparty_type_1 AS reporting_party_type,
        counterparty_type_2 AS counterparty_type,
        collateral_status,
        tenor_bucket,
        is_cleared,

        COUNT(trade_lineage_id) AS trade_count,
        ROUND(SUM(notional_amount_usd), 2) AS gross_notional_usd,
        ROUND(AVG(notional_amount_usd), 2) AS avg_notional_usd,
        ROUND(MAX(notional_amount_usd), 2) AS max_single_trade_usd,

        -- Systemic Risk Indicator: High-Risk Bilateral Uncollateralized Uncleared Exposure
        ROUND(
            SUM(
                CASE 
                    WHEN NOT is_cleared AND collateral_status = 'UNCOLLATERALIZED'
                    THEN notional_amount_usd 
                    ELSE 0 
                END
            ),
            2
        ) AS uncollateralized_uncleared_notional_usd

    FROM enriched_trades
    GROUP BY
        asset_class,
        counterparty_type_1,
        counterparty_type_2,
        collateral_status,
        tenor_bucket,
        is_cleared
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'asset_class',
        'reporting_party_type',
        'counterparty_type',
        'collateral_status',
        'tenor_bucket',
        'is_cleared'
    ]) }} AS counterparty_exposure_key,
    asset_class,
    reporting_party_type,
    counterparty_type,
    collateral_status,
    tenor_bucket,
    is_cleared,
    trade_count,
    gross_notional_usd,
    avg_notional_usd,
    max_single_trade_usd,
    uncollateralized_uncleared_notional_usd,
    -- Ratio of uncollateralized risk relative to segment gross
    ROUND(
        uncollateralized_uncleared_notional_usd * 100.0 / NULLIF(gross_notional_usd, 0),
        2
    ) AS uncollateralized_risk_percentage,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM exposure_aggregation
