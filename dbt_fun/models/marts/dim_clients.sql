{{ config(
    materialized='incremental',
    unique_key='dbt_scd_id'
) }}

select
  card_num,
  first_name,
  last_name,
  gender,
  date_birth,
  job,
  street,
  city,
  state,
  meta_load_ts,
  dbt_valid_from as valid_from,
  dbt_valid_to as valid_to,
  dbt_valid_to is null as is_current,
  dbt_scd_id,
  dbt_updated_at
from {{ ref('clients_snapshot') }}

{% if is_incremental() %}
  where dbt_updated_at > (select max(dbt_updated_at) from {{ this }})
{% endif %}
