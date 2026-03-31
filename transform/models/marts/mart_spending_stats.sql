-- transform/models/marts/mart_spending_stats.sql

with monthly_category_totals as (
    select
        date_trunc('month', transaction_date) as month,
        category,
        sum(amount)                            as total_spend,
        count(*)                               as transaction_count,
        avg(amount)                            as avg_transaction
    from {{ ref('int_categorized') }}
    where transaction_type = 'debit'
    group by 1, 2
),

stats as (
    select
        category,

        -- Central tendency
        round(avg(total_spend), 2)             as mean_monthly_spend,
        round(median(total_spend), 2)          as median_monthly_spend,

        -- Spread
        round(stddev(total_spend), 2)          as stddev_monthly_spend,
        round(min(total_spend), 2)             as min_monthly_spend,
        round(max(total_spend), 2)             as max_monthly_spend,

        -- Derived
        round(max(total_spend)
            - min(total_spend), 2)             as spend_range,
        round(stddev(total_spend)
            / nullif(avg(total_spend), 0), 4)  as coefficient_of_variation,

        sum(transaction_count)                 as total_transactions,
        round(avg(avg_transaction), 2)         as avg_transaction_size

    from monthly_category_totals
    group by category
)

select * from stats
order by mean_monthly_spend desc
