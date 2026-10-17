/*
================================================================================
Test: assert_valid_trade_timestamps
Layer: Data Quality & Regulatory Validation
Description:
    Enforces financial chronology integrity:
    1. execution_timestamp must be in the past or present (no future trades).
    2. maturity_date must be greater than or equal to effective_date.
    3. Trade cannot be reported before it was executed.
================================================================================
*/

SELECT
    dissemination_id,
    execution_timestamp,
    reporting_timestamp,
    effective_date,
    maturity_date
FROM {{ ref('int_trades_enriched') }}
WHERE execution_timestamp > CURRENT_TIMESTAMP()
   OR maturity_date < effective_date
   OR reporting_timestamp < execution_timestamp
