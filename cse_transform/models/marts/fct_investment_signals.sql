with metrics as (
    select * from {{ ref('int_stock_metrics') }}
),

latest_data as (
    select * from metrics
    where trade_date = (select max(trade_date) from metrics)
)

select
    -- 1. Core Identifiers
    stock_symbol,
    trade_date,
    sector,

    -- 2. Price & Return Features
    last_price,
    daily_change_pct,
    opening_gap_pct,
    alpha_vs_sector, -- Is the stock beating its own industry

    -- 3. Momentum Features
    sma_5,
    sma_20,
    sma_50,
    -- Trend Strength: Positive if short-term (5d) is above long-term (20d)
    (sma_5 - sma_20) / nullif(sma_20, 0) as trend_momentum_ratio,

    -- 4. Volatility & Risk Features
    true_range,
    rolling_volatility_20d,
    price_range_proximity, -- 1.0 = Closed at High, 0.0 = Closed at Low

    -- 5. Liquidity & Market Activity
    trade_volume,
    volume_surge_ratio, -- > 1.0 means higher than 20-day average volume
    sector_turnover_share, -- Stock's importance within the sector today
    avg_value_per_trade, -- High values = Institutional activity

    -- 6. Investor Signals
    case 
        when last_price > sma_20 and volume_surge_ratio > 1.2 and alpha_vs_sector > 0 then 'STRONG_BUY'
        when last_price < sma_20 and volume_surge_ratio > 1.2 and alpha_vs_sector < 0 then 'STRONG_SELL'
        when last_price > sma_20 then 'BULLISH_HOLD'
        when last_price < sma_20 then 'BEARISH_HOLD'
        else 'NEUTRAL'
    end as algorithmic_signal,

    -- 7. Fundamental Context
    case 
        when market_cap_total > 50000000000 then 'MEGA_CAP'
        when market_cap_total > 10000000000 then 'LARGE_CAP'
        when market_cap_total > 2000000000 then 'MID_CAP'
        else 'SMALL_CAP'
    end as market_cap_segment

from metrics
order by trade_date desc, stock_symbol asc