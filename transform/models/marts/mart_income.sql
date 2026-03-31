/* models/marts/mart_income.sql
--
-- Mart model that aggregates all income transactions by month and source type.
-- This table powers the income side of the income vs expenses chart in Power BI.
--
-- Grain: one row per income_type per month per source
--
-- Income is identified two ways:
--   1. Transactions flagged as credits (is_debit = false) from the bank source
--      that are not internal transfers — these are true inflows (payroll,
--      interest, reimbursements)
--   2. Card credit transactions that are refunds or cashback, not payments —
--      card payments are excluded because they are not new money arriving,
--      just settling an existing balance
--
-- Filters applied:
--   - Credits only (is_debit = false)
--   - Excludes internal transfers (is_transfer = false)
--   - Excludes card payment rows (is_payment = false)
--   - Respects the start_date variable set in dbt_project.yml
--
-- Power BI usage:
--   - Line chart: transaction_month on axis, total_income as value
--   - KPI card: total_income for selected month vs prior month
--   - Dual-axis chart: combine with mart_monthly_spend for income vs expenses
--   - Slicer: income_type to toggle between payroll, interest, other
*/

with categorized as (

    select * from {{ ref('int_categorized') }}
    where
        is_debit        = false
        and is_transfer = false
        and is_payment  = false
        and transaction_date >= cast('{{ var("start_date") }}' as date)

),

income_typed as (

    select
        *,

        -- Classify income into meaningful types for Power BI filtering
        case
            when description ilike '%PAYROLL%'
                or description ilike '%DIRECT DEPOSIT%'
                then cast('Payroll' as varchar)

            when description ilike '%INTEREST%'
                then 'Interest'

            when description ilike '%REFUND%'
                or description ilike '%RETURN%'
                or description ilike '%CREDIT%'
                then 'Refund'

            when description ilike '%VENMO%'
                or description ilike '%ZELLE%'
                or description ilike '%CASHAPP%'
                then 'Peer transfer'

            when description ilike '%CASHBACK%'
                or description ilike '%REWARDS%'
                then 'Rewards'

            else 'Other income'

        end::varchar as income_type

    from categorized

),

monthly_income as (

    select
        -- Time dimensions
        transaction_month,
        month_label,
        year_number,
        month_number,
        quarter_number,

        -- Income dimensions
        income_type,
        source,

        -- Measures
        sum(absolute_amount)                        as total_income,
        count(*)                                    as transaction_count,
        avg(absolute_amount)                        as avg_transaction_amount,
        max(absolute_amount)                        as largest_transaction_amount

    from income_typed
    group by
        transaction_month,
        month_label,
        year_number,
        month_number,
        quarter_number,
        income_type,
        source

),

with_rank as (

    select
        *,

        -- Month-over-month income change per income type
        total_income - lag(total_income) over (
            partition by income_type, source
            order by transaction_month
        )                                           as mom_income_change,

        -- Total income across all types for this month
        -- Used to calculate pct_of_monthly_income below
        sum(total_income) over (
            partition by transaction_month
        )                                           as total_monthly_income,

        -- Percentage of total income this type represents in this month
        round(
            total_income / nullif(sum(total_income) over (
                partition by transaction_month
            ), 0) * 100,
        2)                                          as pct_of_monthly_income

    from monthly_income

)

select
    -- Keys
    transaction_month,
    month_label,
    year_number,
    month_number,
    quarter_number,
    income_type,
    source,

    -- Core measures
    round(total_income, 2)                          as total_income,
    transaction_count,
    round(avg_transaction_amount, 2)                as avg_transaction_amount,
    round(largest_transaction_amount, 2)            as largest_transaction_amount,

    -- Derived measures
    round(coalesce(mom_income_change, 0), 2)        as mom_income_change,
    round(total_monthly_income, 2)                  as total_monthly_income,
    pct_of_monthly_income

from with_rank
order by
    transaction_month desc,
    total_income desc
