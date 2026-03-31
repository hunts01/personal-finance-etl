/* models/marts/mart_savings_rate.sql
--
-- Mart model that computes monthly savings rate and related metrics by
-- joining the income and spend mart models.
--
-- Grain: one row per month
--
-- Savings rate formula:
--   savings_rate = (total_income - total_spend) / total_income * 100
--
-- A positive savings_rate means income exceeded spend (money saved).
-- A negative savings_rate means spend exceeded income (deficit month).
--
-- Filters applied:
--   - Respects the start_date variable set in dbt_project.yml
--   - Inherits all filters already applied in mart_income and mart_monthly_spend
--     (payments, transfers, and income transactions are already excluded)
--
-- Power BI usage:
--   - KPI card: savings_rate for current month with prior month comparison
--   - Line chart: savings_rate over time to track trend
--   - Dual bar chart: total_income vs total_spend side by side per month
--   - KPI card: cumulative_savings to show running total saved this year
--   - Conditional formatting: red when savings_rate < 0, green when positive
*/

with income as (

    -- Collapse mart_income to one row per month (removing income_type grain)
    select
        transaction_month,
        month_label,
        year_number,
        month_number,
        quarter_number,
        sum(total_income)                           as total_income,
        sum(transaction_count)                      as income_transaction_count
    from {{ ref('mart_income') }}
    group by
        transaction_month,
        month_label,
        year_number,
        month_number,
        quarter_number
),

spend as (

    -- Collapse mart_monthly_spend to one row per month (removing category grain)
    select
        transaction_month,
        sum(total_spend)                            as total_spend,
        sum(transaction_count)                      as spend_transaction_count
    from {{ ref('mart_monthly_spend') }}
    group by
        transaction_month
),

joined as (

    -- Full outer join so months with income but no spend (or vice versa)
    -- are not silently dropped
    select
        coalesce(i.transaction_month,
                 s.transaction_month)               as transaction_month,
        coalesce(i.month_label, 'Unknown')          as month_label,
        coalesce(i.year_number, 0)                  as year_number,
        coalesce(i.month_number, 0)                 as month_number,
        coalesce(i.quarter_number, 0)               as quarter_number,
        coalesce(i.total_income, 0)                 as total_income,
        coalesce(s.total_spend, 0)                  as total_spend,
        coalesce(i.income_transaction_count, 0)     as income_transaction_count,
        coalesce(s.spend_transaction_count, 0)      as spend_transaction_count
    from income i
    full outer join spend s
        on i.transaction_month = s.transaction_month

),

with_savings as (

    select
        *,

        -- Core savings metrics
        round(total_income - total_spend, 2)        as net_savings,

        round(
            (total_income - total_spend)
            / nullif(total_income, 0) * 100,
        2)                                          as savings_rate,

        -- Spending rate — complement of savings rate
        round(
            total_spend
            / nullif(total_income, 0) * 100,
        2)                                          as spending_rate,

        -- Running cumulative savings across all months in the dataset
        round(sum(total_income - total_spend) over (
            order by transaction_month
            rows between unbounded preceding and current row
        ), 2)                                       as cumulative_savings,

        -- Month-over-month changes
        round(
            total_income - lag(total_income) over (
                order by transaction_month
            ),
        2)                                          as mom_income_change,

        round(
            total_spend - lag(total_spend) over (
                order by transaction_month
            ),
        2)                                          as mom_spend_change,

        -- Prior month savings rate for Power BI comparison cards
        lag(
            round(
                (total_income - total_spend)
                / nullif(total_income, 0) * 100,
            2)
        ) over (
            order by transaction_month
        )                                           as prior_month_savings_rate,

        -- Boolean flags for Power BI conditional formatting
        case
            when (total_income - total_spend) >= 0 then true
            else false
        end                                         as is_surplus_month,

        -- Classify the month's financial health for Power BI colour coding
        case
            when total_income = 0
                then 'No income data'
            when (total_income - total_spend)
                / nullif(total_income, 0) >= 0.20
                then 'Healthy'         -- saving 20%+ of income
            when (total_income - total_spend)
                / nullif(total_income, 0) >= 0.05
                then 'On track'        -- saving 5-19% of income
            when (total_income - total_spend)
                / nullif(total_income, 0) >= 0
                then 'Tight'           -- saving 0-4% of income
            else 'Deficit'             -- spending more than earning
        end                                         as financial_health_status,

        income_transaction_count
            + spend_transaction_count               as total_transaction_count

    from joined
    where transaction_month >= cast('{{ var("start_date") }}' as date)

)

select
    -- Keys
    transaction_month,
    month_label,
    year_number,
    month_number,
    quarter_number,

    -- Core income and spend
    total_income,
    total_spend,
    net_savings,

    -- Rates
    savings_rate,
    spending_rate,
    prior_month_savings_rate,

    -- Trends
    cumulative_savings,
    mom_income_change,
    mom_spend_change,

    -- Flags and classifications
    is_surplus_month,
    financial_health_status,

    -- Counts
    income_transaction_count,
    spend_transaction_count,
    total_transaction_count

from with_savings
order by transaction_month desc
