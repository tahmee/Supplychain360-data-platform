-- stg_suppliers.sql

with source as (

    select * from {{ source('raw_s3', 'suppliers') }}

),

renamed as (

    select
        cast(supplier_id as varchar) as supplier_id,
        cast(supplier_name as varchar) as supplier_name,
        cast(category as varchar) as category,
        cast(country as varchar) as country,
        
        _ingestion_timestamp

    from source
    where supplier_id is not null

)

select * from renamed