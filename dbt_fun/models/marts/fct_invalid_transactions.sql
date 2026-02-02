{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trans_num'
) }}

select
  trans_num,
  validation_status,
  meta_load_ts,
  trans_ts,
  card_num,
  amount,
  merchant,
  category,
  is_fraud
from {{ ref('stg_transactions') }}
where validation_status != 'VALID'
{% if is_incremental() %}
  and meta_load_ts > (select max(meta_load_ts) from {{ this }})
{% endif %}
