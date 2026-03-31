-- dim_product.sql
-- SCD Type II dimension. Reads from the dbt snapshot which tracks all historical versions of each product row.
-- is_current = true  - the active version used in current-state reporting
-- is_current = false - historical version; join on surrogate_key + valid_from/to for point-in-time (as-of) reporting

with snapshot as (

    select * from {{ ref('snap_products') }}

),

suppliers as (

    -- Pull only the current supplier record for the denormalised columns.
    -- Supplier history is tracked in dim_supplier
    select
        supplier_id,
        supplier_name,
        category  as supplier_category,
        country   as supplier_country
    from {{ ref('snap_suppliers') }}
    where is_current = true

),

final as (

    select
        -- SCD Type II tracking
        p.surrogate_key,
        p.valid_from,
        p.valid_to,
        p.is_current,
        p.updated_at,

        -- Natural key
        p.product_id,

        -- Slowly changing attributes
        p.product_name,
        p.category,
        p.brand,
        p.unit_price,
        p.supplier_id,

        -- Denormalised from current supplier record
        s.supplier_name,
        s.supplier_category,
        s.supplier_country

    from snapshot p
    left join suppliers s using (supplier_id)

)

select * from final