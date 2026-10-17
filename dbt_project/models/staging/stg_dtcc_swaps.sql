{{
  config(
    materialized='view',
    schema='SILVER',
    tags=['bronze_to_silver', 'staging']
  )
}}

/*
================================================================================
Model: stg_dtcc_swaps
Layer: Bronze to Silver (Staging)
Description:
    Reads raw Iceberg trades directly from Snowflake external table.
    Performs event deduplication over CFTC dissemination chains (NEW/CORRECT/CANCEL),
    casts dates to UTC, and applies sanity filters.
================================================================================
*/

WITH raw_trades AS (
    SELECT
        dissemination_id,
        original_dissemination_id,
        UPPER(TRIM(action_type)) AS action_type,
        execution_timestamp,
        reporting_timestamp,
        UPPER(TRIM(cleared)) AS cleared_raw,
        UPPER(TRIM(collateralization_type)) AS collateralization_type,
        UPPER(TRIM(asset_class)) AS asset_class,
        TRIM(product_id) AS product_id,
        UPPER(TRIM(notional_currency_1)) AS notional_currency_1,
        CAST(notional_amount_1 AS NUMBER(28, 4)) AS notional_amount_1,
        UPPER(TRIM(notional_currency_2)) AS notional_currency_2,
        CAST(notional_amount_2 AS NUMBER(28, 4)) AS notional_amount_2,
        effective_date,
        maturity_date,
        CAST(fixed_rate_1 AS NUMBER(10, 6)) AS fixed_rate_1,
        TRIM(floating_rate_index_1) AS floating_rate_index_1,
        CAST(spread_1 AS NUMBER(10, 4)) AS spread_1,
        UPPER(TRIM(settlement_currency)) AS settlement_currency,
        UPPER(TRIM(counterparty_type_1)) AS counterparty_type_1,
        UPPER(TRIM(counterparty_type_2)) AS counterparty_type_2,
        _ingested_at
    FROM {{ source('dtcc_bronze', 'dtcc_sdr_swaps') }}
),

deduplicated_events AS (
    SELECT
        *,
        -- Resolve unique trade lineage ID across corrections/revisions
        COALESCE(NULLIF(original_dissemination_id, ''), dissemination_id) AS trade_lineage_id,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(NULLIF(original_dissemination_id, ''), dissemination_id)
            ORDER BY reporting_timestamp DESC, execution_timestamp DESC
        ) AS event_rank
    FROM raw_trades
),

filtered_latest AS (
    SELECT
        trade_lineage_id,
        dissemination_id,
        original_dissemination_id,
        action_type,
        execution_timestamp,
        reporting_timestamp,
        -- Standardize cleared flag: C = Cleared, U = Uncleared
        CASE 
            WHEN cleared_raw IN ('C', 'CLEARED', 'TRUE', '1') THEN 'CLEARED'
            ELSE 'UNCLEARED'
        END AS clearing_status,
        -- Standardize collateralization classification
        CASE 
            WHEN collateralization_type IN ('FC', 'FULLY') THEN 'FULLY_COLLATERALIZED'
            WHEN collateralization_type IN ('PC', 'PARTIALLY') THEN 'PARTIALLY_COLLATERALIZED'
            WHEN collateralization_type IN ('OC', 'ONE_WAY') THEN 'ONE_WAY_COLLATERALIZED'
            ELSE 'UNCOLLATERALIZED'
        END AS collateral_status,
        asset_class,
        product_id,
        notional_currency_1,
        notional_amount_1,
        notional_currency_2,
        notional_amount_2,
        effective_date,
        maturity_date,
        fixed_rate_1,
        floating_rate_index_1,
        spread_1,
        settlement_currency,
        counterparty_type_1,
        counterparty_type_2,
        _ingested_at
    FROM deduplicated_events
    WHERE event_rank = 1
      -- Filter out explicit cancelled trades so they do not pollute open positions
      AND action_type != 'CANCEL'
      -- Defensive check: Ensure non-null trade timestamps and positive principal
      AND execution_timestamp IS NOT NULL
      AND notional_amount_1 > 0
)

SELECT * FROM filtered_latest
