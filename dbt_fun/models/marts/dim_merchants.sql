{{ config(
    materialized='incremental',
    unique_key='merchant'
) }}

with latest as (
  select
    merchant,
    merchant_lat,
    merchant_long,
    meta_load_ts
  from {{ ref('int_merchants_latest') }}
)

{% if is_incremental() %}

select l.*
from latest l
left join {{ this }} t
  on l.merchant = t.merchant
where t.merchant is null
  or l.meta_load_ts > t.meta_load_ts

{% else %}

select * from latest

{% endif %}
