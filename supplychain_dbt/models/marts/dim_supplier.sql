-- dim_supplier.sql
-- SCD Type II dimension tracking supplier attribute changes over time.
-- Tracked columns: supplier_name, category, country.

with snapshot as (

    select * from {{ ref('snap_suppliers') }}

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
        supplier_id,

        -- Slowly changing attributes
        supplier_name,
        category,
        country

    from snapshot

)

select * from final