/* models/marts/mart_monthly_spend.sql
--
-- Mart model that aggregates debits by category and month.
-- This is the primary table behind the spending breakdown visuals in Power BI.
--
-- Grain: one row per category per month per source
--
-- Filters applied:
--   - Debits only (is_debit = true)
--   - Excludes card payments (is_payment = false)
--   - Excludes internal transfers (is_transfer = false)
--   - Excludes income transactions (category != 'Income')
--   - Respects the start_date variable set in dbt_project.yml
--
-- Power BI usage:
--   - Bar chart: category on axis, total_spend as value, month_label as legend
--   - Slicer: month_label or transaction_month for date filtering
--   - KPI card: sum of total_spend filtered to current month
--   - Table: all columns for drill-through detail
*/

with categorized as (

    select * from {{ ref('int_categorized') }}
    where
        is_debit        = true
        and is_payment  = false
        and is_transfer = false
        and category    != 'Income'
        and transaction_date >= cast('{{ var("start_date") }}' as date)
),

monthly_category as (

    select
        -- Time dimensions
        transaction_month,
        month_label,
        year_number,
        month_number,
        quarter_number,

        -- Spend dimensions
        category,
        source,

        -- Measures
        sum(absolute_amount)                        as total_spend,
        count(*)                                    as transaction_count,
        avg(absolute_amount)                        as avg_transaction_amount,
        min(absolute_amount)                        as min_transaction_amount,
        max(absolute_amount)                        as max_transaction_amount

    from categorized
    group by
        transaction_month,
        month_label,
        year_number,
        month_number,
        quarter_number,
        category,
        source

),

with_rank as (

    select
        *,

        -- Rank categories by spend within each month (1 = highest spend)
        -- Useful for Power BI top-N filtering without DAX
        rank() over (
            partition by transaction_month
            order by total_spend desc
        )                                           as category_spend_rank,

        -- Month-over-month change in spend for this category
        total_spend - lag(total_spend) over (
            partition by category, source
            order by transaction_month
        )                                           as mom_spend_change,

        -- Percentage of total spend this category represents in this month
        round(
            total_spend / nullif(sum(total_spend) over (
                partition by transaction_month
            ), 0) * 100,
        2)                                          as pct_of_monthly_spend

    from monthly_category

)

select
    -- Keys
    transaction_month,
    month_label,
    year_number,
    month_number,
    quarter_number,
    category,
    source,

    -- Core measures
    round(total_spend, 2)                           as total_spend,
    transaction_count,
    round(avg_transaction_amount, 2)                as avg_transaction_amount,
    round(min_transaction_amount, 2)                as min_transaction_amount,
    round(max_transaction_amount, 2)                as max_transaction_amount,

    -- Derived measures
    category_spend_rank,
    round(coalesce(mom_spend_change, 0), 2)         as mom_spend_change,
    pct_of_monthly_spend

from with_rank
order by
    transaction_month desc,
    total_spend desc
