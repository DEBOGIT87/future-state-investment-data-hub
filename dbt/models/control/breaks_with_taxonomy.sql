{{ config(materialized="view", alias="BREAKS_WITH_TAXONOMY") }}

with breaks as (
  select RUN_ID, SOURCE_SYSTEM, SOURCE_TXN_ID as RECORD_ID, 'NAV_LIKE' as BREAK_DOMAIN, BREAK_CATEGORY, SEVERITY, NOTES
  from {{ ref("recon_nav_like") }}
  union all
  select RUN_ID, SOURCE_SYSTEM, SOURCE_TXN_ID as RECORD_ID, 'CASH_LIKE' as BREAK_DOMAIN, BREAK_CATEGORY, SEVERITY, NOTES
  from {{ ref("recon_cash_like") }}
  union all
  select RUN_ID, SOURCE_SYSTEM, SOURCE_TXN_ID as RECORD_ID, 'REFERENCE' as BREAK_DOMAIN, BREAK_CATEGORY, SEVERITY, NOTES
  from {{ ref("recon_reference_coverage") }}
),
tax as (
  select * from {{ ref("break_taxonomy") }}
)

select
  b.RUN_ID,
  b.SOURCE_SYSTEM,
  b.RECORD_ID,
  b.BREAK_DOMAIN,
  lower(b.BREAK_CATEGORY) as BREAK_CATEGORY,
  coalesce(b.SEVERITY, tax.DEFAULT_SEVERITY) as SEVERITY,
  coalesce(tax.DESCRIPTION, b.NOTES) as DESCRIPTION,
  b.NOTES
from breaks b
left join tax
  on lower(b.BREAK_CATEGORY) = lower(tax.BREAK_CATEGORY)
