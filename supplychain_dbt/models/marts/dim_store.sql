-- dim_store.sql
-- SCD Type II dimension tracking store attribute changes over time.
-- Tracked columns: store_name, city, state, region.

with snapshot as (

    select * from {{ ref('snap_stores') }}

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
        store_id,

        -- Slowly changing attributes
        store_name,
        city,
        state,
        region,

        -- Fixed attribute
        store_open_date

    from snapshot

)

select * from final