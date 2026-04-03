-- fct_inventory.sql
-- Grain: one product stocked per warehouse per day (daily snapshot).

{{
    config(
        materialized         = 'incremental',
        unique_key           = 'inventory_key',
        incremental_strategy = 'merge',
        on_schema_change     = 'sync_all_columns',
    )
}}

with inventory as (

    select * from {{ ref('stg_inventory') }}

    {% if is_incremental() %}
    where _ingestion_timestamp > (select max(_ingestion_timestamp) from {{ this }})
    {% endif %}

),

date_dim as (

    select date_id, full_date
    from {{ ref('dim_date') }}

),

final as (

    select
        -- surrogate key 
        {{ dbt_utils.generate_surrogate_key(['warehouse_id', 'product_id', 'snapshot_date']) }}
            as inventory_key,

        -- keys
        i.warehouse_id,
        i.product_id,
        d.date_id,

        -- measures
        i.quantity_available,
        i.reorder_threshold,
        i.snapshot_date,

        -- derived flag
        i.stock_level,

        -- lineage
        i._ingestion_timestamp,
        i._run_id

    from inventory i
    left join date_dim d
        on i.snapshot_date = d.full_date

)

select * from final