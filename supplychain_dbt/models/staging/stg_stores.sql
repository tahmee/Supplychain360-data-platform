-- stg_stores.sql

with source as (

    select * from {{ source('raw_gsheet', 'gsheet') }}

),

renamed as (

    select
        cast(store_id as varchar) as store_id,
        cast(store_name as varchar) as store_name,
        cast(city as varchar) as city,
        cast(state as varchar) as state,
        cast(region as varchar) as region,
        cast(store_open_date as date)    as store_open_date,
        _ingestion_timestamp

    from source
    where store_id is not null

)

select * from renamed