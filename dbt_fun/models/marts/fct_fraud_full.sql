{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trans_num'
) }}

select
  t.trans_num,
  t.trans_ts,
  t.card_num,
  t.amount,
  t.category,
  t.is_fraud,
  c.first_name,
  c.last_name,
  c.gender,
  c.job,
  c.city as client_city,
  c.state as client_state,
  m.merchant,
  m.merchant_lat,
  m.merchant_long,
  t.meta_load_ts
from {{ ref('stg_transactions') }} t
left join {{ ref('dim_merchants') }} m
  on t.merchant = m.merchant
left join {{ ref('dim_clients') }} c
  on t.card_num = c.card_num
  and t.trans_ts >= c.valid_from
  and (t.trans_ts < c.valid_to or c.valid_to is null)
where t.validation_status = 'VALID'
{% if is_incremental() %}
  and t.meta_load_ts > (select max(meta_load_ts) from {{ this }})
{% endif %}
