-- stg_shipments.sql
-- Cleans shipment records from S3.
-- Daily snapshots of shipment

with source as (

    select * from {{ source('raw_s3', 'shipments') }}

),

renamed as (

    select
        cast(shipment_id as varchar) as shipment_id,
        cast(warehouse_id as varchar) as warehouse_id,
        cast(store_id as varchar) as store_id,
        cast(product_id as varchar) as product_id,
        cast(quantity_shipped as integer) as quantity_shipped,
        cast(shipment_date as date) as shipment_date,
        cast(expected_delivery_date as date) as expected_delivery_date,
        cast(actual_delivery_date as date) as actual_delivery_date,
        cast(carrier as varchar) as carrier,

        -- Derived: was the shipment delivered on time?
        case
            when actual_delivery_date is null then 'PENDING'
            when actual_delivery_date
                 <= expected_delivery_date then 'ON_TIME'
            else 'LATE'
        end as delivery_status,

        _ingestion_timestamp,
        _run_id

    from source
    where shipment_id is not null

)

select * from renamed