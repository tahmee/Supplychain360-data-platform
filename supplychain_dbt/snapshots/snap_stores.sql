{% snapshot snap_stores %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'store_id',
        strategy      = 'check',
        check_cols    = ['store_name', 'city', 'state', 'region'],
    )
}}

select * from {{ ref('stg_stores') }}

{% endsnapshot %}