-- fct_sales.sql
-- Grain: one row per product per transaction.

{{
    config(
        materialized       = 'incremental',
        unique_key         = 'transaction_id',
        incremental_strategy = 'merge',
        on_schema_change   = 'sync_all_columns',
    )
}}

with sales as (

    select * from {{ ref('stg_sales') }}

    {% if is_incremental() %}
    -- Only process rows ingested since the last run
    where _ingestion_timestamp > (select max(_ingestion_timestamp) from {{ this }})
    {% endif %}

),

date_dim as (

    select date_id, full_date
    from {{ ref('dim_date') }}

),

-- Point in time product dimension version
products as (

    select
        surrogate_key  as product_surrogate_key,
        product_id,
        supplier_id,
        unit_price     as listed_unit_price,
        valid_from,
        valid_to
    from {{ ref('dim_product') }}

),

final as (

    select
        -- Surrogate FK to the exact dimension version at time of sale
        p.product_surrogate_key,

        -- Natural keys
        s.transaction_id,
        s.store_id,
        s.product_id,
        p.supplier_id,
        d.date_id,

        -- Measures
        s.quantity_sold,
        s.unit_price,
        s.discount_pct,
        s.sale_amount,

        -- Convenience timestamp
        s.transaction_timestamp,

        -- Lineage
        s._ingestion_timestamp,
        s._run_id,
        s._source_table

    from sales s

    left join date_dim d
        on s.transaction_date = d.full_date

    -- Point in time join: match the product version valid at transaction time
    left join products p
        on  s.product_id = p.product_id
        and s.transaction_timestamp >= p.valid_from
        and (
            s.transaction_timestamp <  p.valid_to
            or p.valid_to is null          
        )

)

select * from final