{{ config(materialized="view", alias="RECON_REFERENCE_COVERAGE") }}

with t as (select * from {{ ref("stg_trade") }}),
a as (select * from {{ ref("stg_security_master") }}),
fx as (select * from {{ ref("stg_fx_rates") }})

select
  t.RUN_ID,
  t.SOURCE_SYSTEM,
  t.SOURCE_TXN_ID,
  t.FUND_ID,
  t.ASSET_ID,
  t.TXN_DATE,
  t.CURRENCY,

  case
    when a.ASSET_ID is null then 'data'
    when t.CURRENCY <> 'USD' and fx.FX_RATE is null then 'data'
    else 'data'
  end as BREAK_CATEGORY,

  case
    when a.ASSET_ID is null then 'high'
    when t.CURRENCY <> 'USD' and fx.FX_RATE is null then 'high'
    else 'med'
  end as SEVERITY,

  case
    when a.ASSET_ID is null then 'Missing security master record for ASSET_ID'
    when t.CURRENCY <> 'USD' and fx.FX_RATE is null then 'Missing FX rate for currency/date (proxy)'
    else 'Reference coverage issue'
  end as NOTES

from t
left join a
  on a.RUN_ID = t.RUN_ID
 and a.SOURCE_SYSTEM = t.SOURCE_SYSTEM
 and a.ASSET_ID = t.ASSET_ID
left join fx
  on fx.RUN_ID = t.RUN_ID
 and fx.SOURCE_SYSTEM = t.SOURCE_SYSTEM
 and fx.FX_DATE = t.TXN_DATE
 and fx.FROM_CCY = t.CURRENCY
 and fx.TO_CCY = 'USD'

where a.ASSET_ID is null
   or (t.CURRENCY <> 'USD' and fx.FX_RATE is null)
