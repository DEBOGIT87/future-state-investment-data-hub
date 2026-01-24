select *
from {{ ref("stg_fx_rates") }}
where FX_RATE <= 0
