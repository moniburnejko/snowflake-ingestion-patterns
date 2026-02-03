with source as (
  select *
  from {{ source('raw', 'raw_clients') }}
),

typed as (
  select
    card_num_raw as card_num,
    first_name_raw as first_name,
    last_name_raw as last_name,
    gender_raw as gender,
    try_to_date(date_birth_raw, 'DD-MM-YYYY') as date_birth,
    job_raw as job,
    street_raw as street,
    city_raw as city,
    state_raw as state,
    meta_filename,
    meta_file_row_number,
    meta_load_ts
  from source
)

select * from typed
