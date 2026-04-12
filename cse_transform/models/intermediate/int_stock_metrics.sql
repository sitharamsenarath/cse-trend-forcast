with prices as (
    select p.*, d.sector, d.market_cap_total, d.beta_value
    from {{ ref('stg_stock_prices') }} p
    left join {{ source('cse_raw', 'dim_stocks') }} d on p.stock_symbol = d.symbol
),

sectors as (
    select * from {{ ref('stg_sector_indices') }}
),

calculations as (
    select 
        p.*,
        s.sector_daily_return_pct,
        s.sector_turnover as total_sector_turnover,

        -- Lagged value for Gap and True Range
        lag(p.last_price) over (partition by p.stock_symbol order by p.trade_date) as prev_close_val,

        -- 1. Momentum: Moving Averages
        avg(p.last_price) over (partition by p.stock_symbol order by p.trade_date rows between 4 preceding and current row) as sma_5,
        avg(p.last_price) over (partition by p.stock_symbol order by p.trade_date rows between 19 preceding and current row) as sma_20,
        avg(p.last_price) over (partition by p.stock_symbol order by p.trade_date rows between 49 preceding and current row) as sma_50,

        -- 2. Volatility: High-Low Spread
        (p.day_high - p.day_low) / nullif(p.day_low, 0) as intraday_volatility,
        stddev(p.daily_change_pct) over (partition by p.stock_symbol order by p.trade_date rows between 19 preceding and current row) as rolling_volatility_20d,
        case 
            when (p.day_high - p.day_low) = 0 then 0
            else (p.last_price - p.day_low) / (p.day_high - p.day_low) 
        end as price_range_proximity,
        
        -- 3. Liquidity: Volume Trends
        avg(p.trade_volume) over (partition by p.stock_symbol order by p.trade_date rows between 19 preceding and current row) as avg_vol_20d,
        p.trade_turnover / nullif(p.trade_count, 0) as avg_value_per_trade

    from prices p
    left join sectors s on p.sector = s.sector_name and p.trade_date = s.trade_date
),

final_features as (
    select
        *,
        -- Alpha calculation
        (daily_change_pct - sector_daily_return_pct) as alpha_vs_sector,
        
        -- Gap Analysis
        (open_price - prev_close_val) / nullif(prev_close_val, 0) as opening_gap_pct,

        -- True Range (TR)
        greatest(
            (day_high - day_low),
            abs(day_high - prev_close_val),
            abs(day_low - prev_close_val)
        ) as true_range,

        -- Market Share of Turnover
        trade_turnover / nullif(total_sector_turnover, 0) as sector_turnover_share,

        -- Volume Surge
        trade_volume / nullif(avg_vol_20d, 0) as volume_surge_ratio,

        -- Turnover Ratio (Fundamental)
        trade_turnover / nullif(market_cap_total, 0) as turnover_to_mcap_ratio

    from calculations
)

select * from final_features