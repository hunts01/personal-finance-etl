/* models/intermediate/int_all_transactions.sql
--
-- Intermediate model that unions bank and card transactions into a single
-- standardised dataset for use by all downstream intermediate and mart models.
--
-- Responsibilities:
--   - Union stg_bank_txns and stg_card_txns into one relation
--   - Assign a globally unique transaction ID across both sources
--   - Derive calendar convenience columns used by every mart
--   - Flag card payment rows so marts can easily exclude them from spend totals
--
-- This model is materialised as ephemeral — it is inlined into downstream
-- queries and does not create a table or view in DuckDB. This keeps the
-- database schema clean while still giving downstream models a clean
-- named reference via {{ ref('int_all_transactions') }}.
--
-- This model does NOT:
--   - Categorise transactions (that happens in int_categorized)
--   - Aggregate or group data (that happens in the mart layer)
--   - Apply spend/income business logic (that happens in the mart layer)
*/

with bank as (

    select
        transaction_id,
        transaction_date,
        description,
        amount,
        is_debit,
        absolute_amount,
        transaction_type,
        null                as provider_category,   -- bank CSV has no category column
        source,
        _loaded_at
    from {{ ref('stg_bank_txns') }}

),

card as (

    select
        transaction_id,
        transaction_date,
        description,
        amount,
        is_debit,
        absolute_amount,
        transaction_type,
        provider_category,
        source,
        _loaded_at
    from {{ ref('stg_card_txns') }}

),

unioned as (

    select * from bank
    union all
    select * from card

),

enriched as (

    select
        -- Globally unique ID combining source prefix and original ID,
        -- preventing collisions if both sources ever share an ID value
        source || '-' || transaction_id             as unique_transaction_id,

        transaction_id,
        transaction_date,
        description,
        provider_category,
        amount,
        is_debit,
        absolute_amount,
        transaction_type,
        source,

        -- Calendar convenience columns
        -- Used by every mart — derived once here rather than repeated in each
        date_trunc('month', transaction_date)       as transaction_month,
        date_trunc('year', transaction_date)        as transaction_year,
        extract('month' from transaction_date)      as month_number,
        extract('year' from transaction_date)       as year_number,
        extract('dow' from transaction_date)        as day_of_week,      -- 0 = Sunday
        extract('quarter' from transaction_date)    as quarter_number,

        -- Human-readable month label for Power BI axis labels
        strftime(transaction_date, '%b %Y')         as month_label,

        -- Payment flag — card payment rows reduce the statement balance
        -- but are not "spend". Marts filter these out with is_payment = false.
        case
            when source = 'card'
                and transaction_type = 'credit'
                and description ilike '%payment%'
            then true
            else false
        end                                         as is_payment,

        -- Transfer flag — internal transfers (e.g. bank to savings)
        -- should not be counted as either income or spend
        case
            when description ilike '%transfer%'
                or description ilike '%zelle%'
                or description ilike '%venmo%'
            then true
            else false
        end                                         as is_transfer,

        _loaded_at

    from unioned

)

select * from enriched
