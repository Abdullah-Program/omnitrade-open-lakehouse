/*
================================================================================
Test: assert_positive_notional_value
Layer: Data Quality & Regulatory Validation
Description:
    Asserts that all swap contracts have strictly positive notional principals.
    Returns failing records (dbt test passes if 0 rows returned).
================================================================================
*/

SELECT
    dissemination_id,
    asset_class,
    notional_currency_1,
    notional_amount_1,
    notional_amount_usd
FROM {{ ref('int_trades_enriched') }}
WHERE notional_amount_usd <= 0
   OR notional_amount_1 <= 0
   OR notional_amount_usd IS NULL
