with metrics as (
    select * from {{ ref('int_stock_metrics') }}
),

latest_data as (
    select * from metrics
    where trade_date = (select max(trade_date) from metrics)
)

select
    stock_symbol,
    trade_date,
    last_price,
    moving_avg_7_day,
    daily_return_pct,

    case    
        when last_price > (moving_avg_7_day * 1.05) then 'SELL / OVERVALUED'
        when last_price < (moving_avg_7_day * 0.95) then 'BUY / UNDERVALUED'
        else 'HOLD / STABLE'
    end as investor_signal

from latest_data