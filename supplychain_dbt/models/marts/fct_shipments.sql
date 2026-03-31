-- fct_shipments.sql
-- Grain: one product shipped from a warehouse to a store per shipment.
--
-- SCD Type II join: product and warehouse versions are matched to the
-- shipment_date so historical analysis reflects the attributes at dispatch.

with shipments as (

    select * from {{ ref('stg_shipments') }}

),

date_dim as (

    select date_id, full_date
    from {{ ref('dim_date') }}

),

products as (

    select
        surrogate_key as product_surrogate_key,
        product_id,
        supplier_id,
        valid_from,
        valid_to
    from {{ ref('dim_product') }}

),

warehouses as (

    select
        surrogate_key as warehouse_surrogate_key,
        warehouse_id,
        valid_from,
        valid_to
    from {{ ref('dim_warehouse') }}

),

final as (

    select
        -- Surrogate FKs (point-in-time dimension versions)
        p.product_surrogate_key,
        w.warehouse_surrogate_key,

        -- Natural keys
        sh.shipment_id,
        sh.warehouse_id,
        sh.store_id,
        sh.product_id,
        p.supplier_id,
        d.date_id,

        -- Measures / attributes
        sh.quantity_shipped,
        sh.carrier,
        sh.shipment_date,
        sh.expected_delivery_date,
        sh.actual_delivery_date,
        sh.delivery_status,

        datediff(
            'day',
            sh.expected_delivery_date,
            coalesce(sh.actual_delivery_date, current_date())
        ) as delivery_days_variance,

        -- Lineage
        sh._ingestion_timestamp,
        sh._run_id

    from shipments sh

    left join date_dim d
        on sh.shipment_date = d.full_date

    -- Point-in-time product version at shipment date
    left join products p
        on  sh.product_id = p.product_id
        and sh.shipment_date >= cast(p.valid_from as date)
        and (
            sh.shipment_date <  cast(p.valid_to as date)
            or p.valid_to is null
        )

    -- Point-in-time warehouse version at shipment date
    left join warehouses w
        on  sh.warehouse_id = w.warehouse_id
        and sh.shipment_date >= cast(w.valid_from as date)
        and (
            sh.shipment_date <  cast(w.valid_to as date)
            or w.valid_to is null
        )

)

select * from final