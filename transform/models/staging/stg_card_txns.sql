/* models/staging/stg_card_txns.sql
 Staging model for raw credit card transaction data.

 Responsibilities:
   - Rename columns to the project-wide naming convention
   - Cast all columns to their correct data types
   - Derive a clean signed amount (negative = money out, positive = money in)
   - Preserve the card provider's merchant_category for use in int_categorized
   - Filter out rows that cannot be used downstream (null IDs, unparseable dates)
   - Add a source identifier for traceability after the union in intermediate

 Key differences from stg_bank_txns:
   - Uses post_date instead of date
   - Uses merchant_name instead of description
   - Has a merchant_category column the bank source does not have
   - Card payments (e.g. "PAYMENT RECEIVED") are credits that reduce balance —
     these are excluded from spend aggregations in the mart layer, not here

 This model does NOT:
   - Categorise transactions (that happens in int_categorized)
   - Join to any other source (that happens in int_all_transactions)
   - Apply any business logic or aggregation
*/

with source as (
    select * from {{ source('raw', 'raw_card_transactions') }}
),

renamed as (
    select
        -- Identity
        transaction_id as transaction_id,
        -- Dates
        -- Card exports use post_date (settlement date) rather than
        -- transaction date. We rename to transaction_date for consistency
        -- with stg_bank_txns so the union in int_all_transactions works cleanly.
        cast(post_date as date) as transaction_date,

        -- Description — card exports use merchant_name
        upper(trim(merchant_name)) as description,

        -- Merchant category as provided by the card network.
        -- May be null for some transactions (e.g. fees, payments).
        -- int_categorized will use this as a first-pass category before
        -- falling back to keyword matching on the description.
        initcap(trim(merchant_category)) as provider_category,

        -- Amount
        -- Card CSVs store debits (purchases) as positive numbers and
        -- credits (payments, refunds) as negative numbers — the opposite
        -- convention to the bank CSV. We flip the sign here so that
        -- purchases are negative and payments are positive, matching the
        -- bank convention and making downstream aggregations consistent.
        cast(amount as decimal(12, 2)) * -1 as amount,
        case
            when cast(amount as decimal(12, 2)) > 0 then true
            else false
        end as is_debit,
        abs(cast(amount as decimal(12, 2))) as absolute_amount,

        -- Transaction type — normalise to lower case
        lower(trim(transaction_type)) as transaction_type,

        -- Source identifier — carried through to marts for lineage
        'card' as source,

        -- Audit column
        current_timestamp as _loaded_at
    from source
),

filtered as (
    -- Remove rows that would cause joins or aggregations to fail downstream.
    -- Note: card payment rows (e.g. "PAYMENT RECEIVED") are NOT filtered here —
    -- they are valid transactions and are excluded in the mart layer where the
    -- business logic for what counts as "spend" is defined.

    select *
    from renamed
    where
        transaction_id is not null
        and transaction_date is not null
        and amount is not null

)

select * from filtered
