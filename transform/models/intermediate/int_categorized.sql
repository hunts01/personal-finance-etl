/* models/intermediate/int_categorized.sql
--
-- Intermediate model that assigns a spend category to every transaction.
--
-- Categorisation strategy (applied in priority order):
--   1. If the card provider supplied a category, use it directly
--   2. If the description matches a keyword pattern, use that category
--   3. Fall back to "Other" for anything unmatched
--
-- This model is materialised as ephemeral — inlined into downstream mart
-- queries rather than written to DuckDB as a table or view.
--
-- To add a new category or keyword:
--   - Add a new WHEN clause to the keyword_match CTE below
--   - Keep keywords in upper case — descriptions are already uppercased
--     in the staging models so comparisons are case-insensitive by design
--
-- This model does NOT:
--   - Aggregate or group data (that happens in the mart layer)
--   - Filter out payments or transfers (marts handle that with the flags
--     set in int_all_transactions)
*/

with transactions as (
  
    select * from {{ ref('int_all_transactions') }}
),

keyword_match as (

    select
        unique_transaction_id,
        -- Priority 1: trust the card provider's category when available
        -- Priority 2: keyword match on description
        -- Priority 3: fall back to Other
        case
            when provider_category is not null
                and provider_category != ''
                and provider_category != 'Unknown'
                then provider_category

            -- Groceries
            when description ilike '%WALMART%'          then 'Groceries'
            when description ilike '%KROGER%'           then 'Groceries'
            when description ilike '%WHOLEFDS%'         then 'Groceries'
            when description ilike '%WHOLE FOODS%'      then 'Groceries'
            when description ilike '%PUBLIX%'           then 'Groceries'
            when description ilike '%TRADER JOE%'       then 'Groceries'
            when description ilike '%ALDI%'             then 'Groceries'
            when description ilike '%COSTCO%'           then 'Groceries'
            when description ilike '%SAM''S CLUB%'      then 'Groceries'
            when description ilike '%TARGET%'           then 'Groceries'

            -- Dining
            when description ilike '%MCDONALD%'         then 'Dining'
            when description ilike '%CHIPOTLE%'         then 'Dining'
            when description ilike '%STARBUCKS%'        then 'Dining'
            when description ilike '%CHICK-FIL-A%'      then 'Dining'
            when description ilike '%CHICK FIL A%'      then 'Dining'
            when description ilike '%OLIVE GARDEN%'     then 'Dining'
            when description ilike '%CHEESECAKE%'       then 'Dining'
            when description ilike '%SUBWAY%'           then 'Dining'
            when description ilike '%DOMINO%'           then 'Dining'
            when description ilike '%PIZZA%'            then 'Dining'
            when description ilike '%DOORDASH%'         then 'Dining'
            when description ilike '%GRUBHUB%'          then 'Dining'
            when description ilike '%UBER EATS%'        then 'Dining'
            when description ilike '%PANERA%'           then 'Dining'
            when description ilike '%TACO BELL%'        then 'Dining'

            -- Transport
            when description ilike '%SHELL%'            then 'Transport'
            when description ilike '%CHEVRON%'          then 'Transport'
            when description ilike '%BP OIL%'           then 'Transport'
            when description ilike '%RACETRAC%'         then 'Transport'
            when description ilike '%EXXON%'            then 'Transport'
            when description ilike '%SUNOCO%'           then 'Transport'
            when description ilike '%UBER%'             then 'Transport'
            when description ilike '%LYFT%'             then 'Transport'
            when description ilike '%PARKING%'          then 'Transport'
            when description ilike '%SUNPASS%'          then 'Transport'
            when description ilike '%TOLL%'             then 'Transport'

            -- Subscriptions
            when description ilike '%NETFLIX%'          then 'Subscriptions'
            when description ilike '%SPOTIFY%'          then 'Subscriptions'
            when description ilike '%HULU%'             then 'Subscriptions'
            when description ilike '%DISNEY%'           then 'Subscriptions'
            when description ilike '%APPLE.COM/BILL%'   then 'Subscriptions'
            when description ilike '%AMAZON PRIME%'     then 'Subscriptions'
            when description ilike '%YOUTUBE%'          then 'Subscriptions'
            when description ilike '%ADOBE%'            then 'Subscriptions'
            when description ilike '%MICROSOFT%'        then 'Subscriptions'
            when description ilike '%DROPBOX%'          then 'Subscriptions'

            -- Utilities
            when description ilike '%ELECTRIC%'         then 'Utilities'
            when description ilike '%FPL%'              then 'Utilities'
            when description ilike '%DUKE ENERGY%'      then 'Utilities'
            when description ilike '%COMCAST%'          then 'Utilities'
            when description ilike '%AT&T%'             then 'Utilities'
            when description ilike '%VERIZON%'          then 'Utilities'
            when description ilike '%T-MOBILE%'         then 'Utilities'
            when description ilike '%WATER%'            then 'Utilities'
            when description ilike '%INTERNET%'         then 'Utilities'
            when description ilike '%XFINITY%'          then 'Utilities'

            -- Insurance
            when description ilike '%PROGRESSIVE%'      then 'Insurance'
            when description ilike '%GEICO%'            then 'Insurance'
            when description ilike '%STATE FARM%'       then 'Insurance'
            when description ilike '%ALLSTATE%'         then 'Insurance'
            when description ilike '%INSURANCE%'        then 'Insurance'

            -- Health
            when description ilike '%CVS%'              then 'Health'
            when description ilike '%WALGREENS%'        then 'Health'
            when description ilike '%PHARMACY%'         then 'Health'
            when description ilike '%DOCTOR%'           then 'Health'
            when description ilike '%MEDICAL%'          then 'Health'
            when description ilike '%DENTAL%'           then 'Health'
            when description ilike '%OPTUM%'            then 'Health'

            -- Fitness
            when description ilike '%PLANET FITNESS%'   then 'Fitness'
            when description ilike '%GOLD''S GYM%'      then 'Fitness'
            when description ilike '%LA FITNESS%'       then 'Fitness'
            when description ilike '%GYM%'              then 'Fitness'
            when description ilike '%PELOTON%'          then 'Fitness'

            -- Shopping
            when description ilike '%AMAZON%'           then 'Shopping'
            when description ilike '%BEST BUY%'         then 'Shopping'
            when description ilike '%HOME DEPOT%'       then 'Shopping'
            when description ilike '%LOWES%'            then 'Shopping'
            when description ilike '%IKEA%'             then 'Shopping'
            when description ilike '%WAYFAIR%'          then 'Shopping'
            when description ilike '%EBAY%'             then 'Shopping'

            -- Travel
            when description ilike '%DELTA%'            then 'Travel'
            when description ilike '%UNITED AIR%'       then 'Travel'
            when description ilike '%AMERICAN AIR%'     then 'Travel'
            when description ilike '%SOUTHWEST%'        then 'Travel'
            when description ilike '%MARRIOTT%'         then 'Travel'
            when description ilike '%HILTON%'           then 'Travel'
            when description ilike '%AIRBNB%'           then 'Travel'
            when description ilike '%HOTEL%'            then 'Travel'

            -- Income
            when description ilike '%PAYROLL%'          then 'Income'
            when description ilike '%DIRECT DEPOSIT%'   then 'Income'
            when description ilike '%INTEREST PAYMENT%' then 'Income'

            -- Fees
            when description ilike '%FEE%'              then 'Fees'
            when description ilike '%FOREIGN TRANSACTION%' then 'Fees'
            when description ilike '%LATE PAYMENT%'     then 'Fees'
            when description ilike '%OVERDRAFT%'        then 'Fees'

            else 'Other'

        end::varchar as category

    from transactions

),

final as (

    select
        t.*,
        k.category,

        -- Flag transactions that fell through to Other for monitoring.
        -- A high rate of uncategorised transactions is a signal to add
        -- more keyword rules above.
        case
            when k.category = 'Other' then true
            else false
        end::boolean as is_uncategorised

    from transactions t
    left join keyword_match k
        on t.unique_transaction_id = k.unique_transaction_id

)

select * from final
