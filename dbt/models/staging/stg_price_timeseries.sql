{{ config(alias="STG_PRICE_TIMESERIES") }}

{% set run_id = var("run_id") %}
{% set raw_dir = var("raw_dir") %}

with src as (

  {% if target.type == "duckdb" %}
    select * from read_csv_auto(
      '{{ raw_dir ~ "/price_timeseries_extract_" ~ run_id ~ ".csv" }}',
      header=true
    )
  {% else %}
    select * from {{ source('bronze','PRICE_TIMESERIES') }}
  {% endif %}

)

select
  RUN_ID,
  SOURCE_SYSTEM,
  {{ try_to_date("PRICE_DATE") }} as PRICE_DATE,
  ASSET_ID,
  {{ try_to_float("PRICE") }} as PRICE,
  PRICE_CCY
from src
