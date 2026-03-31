-- stg_products.sql

with source as (

    select * from {{ source('raw_s3', 'products') }}

),

renamed as (

    select
        cast(product_id    as varchar)        as product_id,
        cast(product_name  as varchar)        as product_name,
        cast(category      as varchar)        as category,
        cast(brand         as varchar)        as brand,
        cast(supplier_id   as varchar)        as supplier_id,
        cast(unit_price    as numeric(10,2))  as unit_price,

        _ingestion_timestamp

    from source
    where product_id is not null

)

select * from renamed