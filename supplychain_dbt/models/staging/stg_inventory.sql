-- stg_inventory.sql
-- Daily warehouse inventory snapshots.

with source as (

    select * from {{ source('raw_s3', 'inventory') }}

),

renamed as (

    select
        cast("warehouse_id" as varchar) as warehouse_id,
        cast("product_id" as varchar) as product_id,
        cast("quantity_available" as integer) as quantity_available,
        cast("reorder_threshold" as integer) as reorder_threshold,
        cast("snapshot_date" as date) as snapshot_date,

        case
            when "quantity_available" = 0 then 'OUT_OF_STOCK'
            when "quantity_available" <= "reorder_threshold" then 'LOW_STOCK'
            else 'IN_STOCK'
        end as stock_level,

        "_ingestion_timestamp" as _ingestion_timestamp,
        "_run_id" as _run_id

    from source
    where "warehouse_id" is not null
      and "product_id" is not null

)

select * from renamed
