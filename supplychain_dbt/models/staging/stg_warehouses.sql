-- stg_warehouses.sql

with source as (

    select * from {{ source('raw_s3', 'warehouses') }}

),

renamed as (

    select
        cast("warehouse_id" as varchar) as warehouse_id,
        cast("city" as varchar) as city,
        cast("state" as varchar) as state,

        "_ingestion_timestamp" as _ingestion_timestamp

    from source
    where "warehouse_id" is not null

)

select * from renamed
