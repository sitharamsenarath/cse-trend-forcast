with source as (
    select * from {{ source('cse_raw', 'fact_sector_indices') }}
)

select
    index_name as sector_name,
    index_code,
    index_value,
    sector_turnover,
    sector_volume,
    change_percentage as sector_daily_return_pct,
    extracted_at::date as trade_date
    
from source