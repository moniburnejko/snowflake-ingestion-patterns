with cte_clients as (
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
      meta_load_ts as valid_from,
      meta_filename,
      meta_file_row_number,
      meta_load_ts    
    from {{ ref('stg_clients') }}
),
deduped_clients as (
  {{ dbt_utils.deduplicate(
      relation='cte_clients',
      partition_by='card_num, meta_load_ts',
      order_by='meta_file_row_number desc'
   )
 }}
)
select * from deduped_clients