{{ config(materialized="view", alias="RECON_CASH_LIKE") }}

with t as (select * from {{ ref("stg_trade") }})

select
  RUN_ID,
  SOURCE_SYSTEM,
  SOURCE_TXN_ID,
  FUND_ID,
  ASSET_ID,
  TXN_DATE,
  SETTLE_DATE,

  case
    when SETTLE_DATE is null then 'data'
    when SETTLE_DATE <> TXN_DATE then 'timing'
    else 'timing'
  end as BREAK_CATEGORY,

  case
    when SETTLE_DATE is null then 'high'
    when SETTLE_DATE <> TXN_DATE then 'med'
    else 'low'
  end as SEVERITY,

  case
    when SETTLE_DATE is null then 'Missing settle date'
    else 'Settle date differs from trade date (timing proxy break)'
  end as NOTES

from t
where SETTLE_DATE is null
   or SETTLE_DATE <> TXN_DATE
