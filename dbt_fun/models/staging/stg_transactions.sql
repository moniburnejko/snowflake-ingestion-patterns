with source as (
  select *
  from {{ source('raw', 'raw_trans') }}
),

typed as (
  select
    trans_num_raw as trans_num,
    coalesce(
      try_to_timestamp(trans_date_raw, 'DD-MM-YYYY HH24:MI:SS'),
      try_to_timestamp(trans_date_raw, 'DD-MM-YYYY HH24:MI')
    ) as trans_ts,
    card_num_raw as card_num,
    try_to_decimal(amount_raw, 18, 2) as amount,
    merchant_raw as merchant,
    category_raw as category,
    try_to_boolean(is_fraud_raw) as is_fraud,
    meta_filename,
    meta_file_row_number,
    meta_load_ts
  from source
),

validated as (
  select
    *,
    case
      when trans_ts is null then 'INVALID_DATE'
      when amount is null then 'INVALID_AMOUNT'
      when is_fraud is null then 'INVALID_IS_FRAUD'
      else 'VALID'
    end as validation_status
  from typed
)

select * from validated
