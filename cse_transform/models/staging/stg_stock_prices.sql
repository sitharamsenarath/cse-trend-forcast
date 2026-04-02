with source as (
    select * from {{ source('cse_raw', 'fact_stock_prices') }}
)

select
    symbol as stock_symbol,

    price::decimal(12,2) as last_price,
    high::decimal(12,2) as day_high,
    low::decimal(12,2) as day_low,
    change_percentage::decimal(12,4) as daily_change_pct,

    extracted_at as recorded_at,
    extracted_at::date as trade_date

from source
where price > 0