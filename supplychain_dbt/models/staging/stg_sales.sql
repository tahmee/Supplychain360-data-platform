-- stg_sales.sql
-- Cleans and types raw sales transaction data from Supabase.
-- Source tables are date-partitioned (sales_YYYY_MM_DD); we union them all.

with source as (

    select * from {{ source('raw_postgres', 'sales') }}

),

renamed as (

    select
        cast(transaction_id          as varchar)   as transaction_id,
        cast(store_id                as varchar)   as store_id,
        cast(product_id              as varchar)   as product_id,
        cast(quantity_sold           as integer)   as quantity_sold,
        cast(unit_price              as numeric(10,2)) as unit_price,
        cast(discount_pct            as numeric(5,4))  as discount_pct,
        cast(sale_amount             as numeric(10,2)) as sale_amount,
        cast(transaction_timestamp   as timestamp) as transaction_timestamp,
        cast(transaction_timestamp   as date)      as transaction_date,

        -- metadata passthrough
        _ingestion_timestamp,
        _run_id,
        _source_table

    from source
    where transaction_id is not null

)

select * from renamed