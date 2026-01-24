{{ config(materialized="view", alias="RECON_NAV_LIKE") }}

with t as (select * from {{ ref("stg_trade") }}),
p as (select * from {{ ref("stg_price_timeseries") }})

select
  t.RUN_ID,
  t.SOURCE_SYSTEM,
  t.SOURCE_TXN_ID,
  t.FUND_ID,
  t.ASSET_ID,
  t.TXN_DATE,

  p.PRICE as MARKET_PRICE,
  t.PRICE as TRADE_PRICE,
  (t.PRICE - p.PRICE) as PRICE_DIFF,

  case
    when p.PRICE is null then 'data'
    when abs(t.PRICE - p.PRICE) <= 0.0001 then 'rounding'
    else 'logic'
  end as BREAK_CATEGORY,

  case
    when p.PRICE is null then 'high'
    when abs(t.PRICE - p.PRICE) <= 0.01 then 'low'
    else 'med'
  end as SEVERITY,

  case
    when p.PRICE is null then 'Missing market price for asset/date'
    else 'Trade price differs from market price on trade date'
  end as NOTES

from t
left join p
  on p.RUN_ID = t.RUN_ID
 and p.SOURCE_SYSTEM = t.SOURCE_SYSTEM
 and p.ASSET_ID = t.ASSET_ID
 and p.PRICE_DATE = t.TXN_DATE

where p.PRICE is null
   or abs(t.PRICE - p.PRICE) > 0.0001
