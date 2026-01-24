select *
from {{ ref("stg_trade") }}
where FUND_ID is null or ASSET_ID is null or SOURCE_TXN_ID is null
