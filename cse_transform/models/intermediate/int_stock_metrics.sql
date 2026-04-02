with stock_data as (
    select * from {{ ref('stg_stock_prices') }}
),

calculations as (
    select 
        stock_symbol,
        trade_date,
        last_price,

        lag(last_price) over (partition by stock_symbol order by trade_date) as prev_price,

        avg(last_price) over (
            partition by stock_symbol
            order by trade_date
            rows between 6 preceding and current row
        ) as moving_avg_7_day

    from stock_data
)

select
    stock_symbol,
    trade_date,
    last_price,
    moving_avg_7_day,

    case
        when prev_price is not null then (last_price - prev_price) / prev_price
        else 0
    end as daily_return_pct
from calculations