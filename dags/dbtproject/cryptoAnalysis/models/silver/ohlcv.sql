{{ config(
    materialized='incremental',
    unique_key=['symbol', 'interval', 'open_time'],
    on_conflict='ignore'
) }}

select
    symbol,
    interval,
    open_time,
    close_time,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    trades_count,
    taker_buy_base_volume,
    taker_buy_quote_volume,
    now() updated_at
from {{ source('raw', 'rd_ohlcv') }}

{% if is_incremental() %}
  where open_time > coalesce(
    (select max(open_time) from {{ this }}),
    timestamp '1970-01-01'
  )
{% endif %}