with source as (
    select * from {{ source('cse_raw', 'fact_stock_prices') }}
)

select
    symbol as stock_symbol,
    price::decimal(12,2) as last_price,
    open_price::decimal(12,2) as open_price,
    high::decimal(12,2) as day_high,
    low::decimal(12,2) as day_low,
    prev_close::decimal(12,2) as prev_close,
    volume as trade_volume,
    turnover as trade_turnover,
    trade_count,
    change_percentage::decimal(12,4) as daily_change_pct,
    extracted_at::date as trade_date 

from source
where price > 0