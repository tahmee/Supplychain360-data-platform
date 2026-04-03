-- dim_warehouse.sql
-- SCD Type II dimension tracking warehouse attribute changes over time.
-- Tracked columns: city, state

with snapshot as (

    select * from {{ ref('snap_warehouses') }}

),

final as (

    select
        -- SCD Type II tracking
        surrogate_key,
        valid_from,
        valid_to,
        is_current,
        updated_at,

        -- Natural key
        warehouse_id,

        -- Slowly changing attributes
        city,
        state

    from snapshot

)

select * from final