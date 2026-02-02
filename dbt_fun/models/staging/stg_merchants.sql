with source as (
  select *
  from {{ source('raw', 'raw_merch') }}
),

typed as (
  select
    merchant_raw as merchant,
    try_to_decimal(merchant_lat_raw, 11, 8) as merchant_lat,
    try_to_decimal(merchant_long_raw, 11, 8) as merchant_long,
    meta_filename,
    meta_file_row_number,
    meta_load_ts
  from source
)
select * from typed