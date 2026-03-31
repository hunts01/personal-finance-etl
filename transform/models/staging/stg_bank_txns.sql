/* models/staging/stg_bank_txns.sql
 Staging model for raw bank transaction data.

 Responsibilities:
   - Rename columns to the project-wide naming convention
   - Cast all columns to their correct data types
   - Derive a clean signed amount (negative = money out, positive = money in)
   - Filter out rows that cannot be used downstream (null IDs, unparseable dates)
   - Add a source identifier for traceability after the union in intermediate

 This model does NOT:
   - Categorise transactions (that happens in int_categorized)
   - Join to any other source (that happens in int_all_transactions)
   - Apply any business logic or aggregation
*/

with source as (
    select * from {{ source('raw', 'raw_bank_transactions') }}
),

renamed as (
    select
        -- Identity
        transaction_id as transaction_id,
        -- Dates
        cast("date" as date) as transaction_date,
        -- Description — trim whitespace, normalise to upper case
        upper(trim(description)) as description,

        -- Amount
        -- Bank CSVs store debits as negative numbers and credits as positive.
        -- We preserve the sign here and derive is_debit for convenience.
        cast(amount as double)              as amount,

        case
            when cast(amount as double) < 0 then true
            else false
        end                                         as is_debit,

        abs(cast(amount as double))         as absolute_amount,

        -- Transaction type — normalise to lower case
        lower(trim(transaction_type))               as transaction_type,

        -- Running balance as reported by the bank — useful for reconciliation
        -- but not used in mart aggregations
        --cast(balance as decimal(12, 2))             as reported_balance,

        -- Source identifier — carried through to marts for lineage
        'bank'                                      as source,

        -- Audit columns
        current_timestamp                           as _loaded_at

    from source
),

filtered as (
    -- Remove rows that would cause joins or aggregations to fail downstream.
    -- Rows removed here are logged as a dbt test failure, not silently dropped.
    select *
    from renamed
    where
        transaction_id is not null
        and transaction_date is not null
        and amount is not null
)

select * from filtered
