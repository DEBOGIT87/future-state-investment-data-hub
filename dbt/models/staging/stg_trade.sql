{{ config(alias="STG_TRADE") }}

{% set run_id = var("run_id") %}
{% set raw_dir = var("raw_dir") %}

with src as (

  {% if target.type == "duckdb" %}
    select * from read_csv_auto(
      '{{ raw_dir ~ "/trade_extract_" ~ run_id ~ ".csv" }}',
      header=true
    )
  {% else %}
    select * from {{ source('bronze','TRADE') }}
  {% endif %}

)

select
  RUN_ID,
  SOURCE_SYSTEM,
  SOURCE_TXN_ID,
  FUND_ID,
  ASSET_ID,
  TXN_TYPE,
  {{ try_to_date("TXN_DATE") }} as TXN_DATE,
  {{ try_to_date("SETTLE_DATE") }} as SETTLE_DATE,
  {{ try_to_float("QUANTITY") }} as QUANTITY,
  {{ try_to_float("PRICE") }} as PRICE,
  CURRENCY
from src
