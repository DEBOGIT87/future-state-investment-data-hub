{{ config(alias="STG_FUND_MASTER") }}

{% set run_id = var("run_id") %}
{% set raw_dir = var("raw_dir") %}

with src as (

  {% if target.type == "duckdb" %}
    select * from read_csv_auto(
      '{{ raw_dir ~ "/fund_master_extract_" ~ run_id ~ ".csv" }}',
      header=true
    )
  {% else %}
    select * from {{ source('bronze','FUND_MASTER') }}
  {% endif %}

)

select
  RUN_ID,
  SOURCE_SYSTEM,
  FUND_ID,
  FUND_NAME,
  BASE_CURRENCY,
  {{ try_to_date("INCEPTION_DATE") }} as INCEPTION_DATE,
  ACCOUNTING_BASIS
from src
