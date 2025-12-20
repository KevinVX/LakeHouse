{{ config(
    materialized='incremental',
    unique_key=['symbol', 'interval', 'open_time']
) }}

with ohlcv_data as (
    SELECT
        symbol,
        interval,
        open_time,
        close AS current_close,

        LAG(close) OVER (
            PARTITION BY symbol, interval
            ORDER BY open_time
        ) AS prev_close,

        ROUND(
            (close - LAG(close) OVER (
                PARTITION BY symbol, interval
                ORDER BY open_time
            ))
            / LAG(close) OVER (
                PARTITION BY symbol, interval
                ORDER BY open_time
            ) * 100,
            2
        ) AS pct_change
    FROM {{ ref('ohlcv') }}
    WHERE open_time > COALESCE(
        (SELECT MAX(open_time) - INTERVAL '1 hour' FROM {{ this }}),
        TIMESTAMP '1970-01-01'
    ) 
)
select * from ohlcv_data

{% if is_incremental() %}
  WHERE open_time > COALESCE(
    (SELECT MAX(open_time) FROM {{ this }}),
    TIMESTAMP '1970-01-01'
  )
{% endif %}