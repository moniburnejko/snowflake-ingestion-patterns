with cte_merch as (
  select
    merchant,
    merchant_lat,
    merchant_long,
    meta_filename,
    meta_file_row_number,
    meta_load_ts
  from {{ ref('stg_merchants') }}
),

deduped_merch as (
  {{ dbt_utils.deduplicate(
      relation='cte_merch',
      partition_by='merchant',
      order_by='meta_load_ts desc, meta_file_row_number desc'
  ) }}
)

select * from deduped_merch
