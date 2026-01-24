{{ config(alias="STG_SECURITY_MASTER") }}

{% set run_id = var("run_id") %}
{% set raw_dir = var("raw_dir") %}

with src as (

  {% if target.type == "duckdb" %}
    select * from read_csv_auto(
      '{{ raw_dir ~ "/security_master_extract_" ~ run_id ~ ".csv" }}',
      header=true
    )
  {% else %}
    select * from {{ source('bronze','SECURITY_MASTER') }}
  {% endif %}

)

select
  RUN_ID,
  SOURCE_SYSTEM,
  ASSET_ID,
  ASSET_TYPE,
  CURRENCY,
  ID_ISIN,
  {{ try_to_date("ISSUE_DATE") }} as ISSUE_DATE,
  {{ try_to_date("MATURITY_DATE") }} as MATURITY_DATE,
  TICKER,
  {{ try_to_float("COUPON_RATE") }} as COUPON_RATE,
  PAY_FREQ,
  DAY_COUNT,
  RESET_FREQUENCY,
  INDEX_NAME,
  UNDERLYING,
  {{ try_to_float("STRIKE") }} as STRIKE
from src
