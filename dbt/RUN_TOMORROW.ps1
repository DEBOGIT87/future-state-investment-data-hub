# Run this from the dbt folder
dbt deps
dbt seed
dbt run --vars "run_id: 20260118_201244_c80818"
