{{ config(alias="STG_FX_RATES") }}

{% set run_id = var("run_id") %}
{% set raw_dir = var("raw_dir") %}

with src as (

  {% if target.type == "duckdb" %}
    select * from read_csv_auto(
      '{{ raw_dir ~ "/fx_rates_extract_" ~ run_id ~ ".csv" }}',
      header=true
    )
  {% else %}
    select * from {{ source('bronze','FX_RATES') }}
  {% endif %}

)

select
  RUN_ID,
  SOURCE_SYSTEM,
  {{ try_to_date("FX_DATE") }} as FX_DATE,
  FROM_CCY,
  TO_CCY,
  {{ try_to_float("FX_RATE") }} as FX_RATE
from src
