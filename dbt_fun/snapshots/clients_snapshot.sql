{% snapshot clients_snapshot %}

{{ config(
    unique_key='card_num',
    strategy='timestamp',
    updated_at='meta_load_ts'
) }}

select * from {{ ref('int_clients_deduped') }}

{% endsnapshot %}