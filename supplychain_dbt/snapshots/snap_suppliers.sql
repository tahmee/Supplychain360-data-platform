{% snapshot snap_suppliers %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'supplier_id',
        strategy      = 'check',
        check_cols    = ['supplier_name', 'category', 'country'],
    )
}}

select * from {{ ref('stg_suppliers') }}

{% endsnapshot %}