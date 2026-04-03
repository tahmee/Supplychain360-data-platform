{% snapshot snap_products %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'product_id',
        strategy      = 'check',
        check_cols    = ['product_name', 'category', 'brand', 'supplier_id', 'unit_price'],
    )
}}

select * from {{ ref('stg_products') }}

{% endsnapshot %}