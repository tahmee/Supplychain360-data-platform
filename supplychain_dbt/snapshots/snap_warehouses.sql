{% snapshot snap_warehouses %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'warehouse_id',
        strategy      = 'check',
        check_cols    = ['city', 'state'],
    )
}}

select * from {{ ref('stg_warehouses') }}

{% endsnapshot %}