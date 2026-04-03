-- dim_date.sql
-- Date dimension generated with a recursive CTE.

with date_spine as (

    select
        dateadd('day', seq4(), '{{ var("start_date") }}'::date) as calendar_date
    from table(generator(rowcount => 2000))

),

final as (

    select
        to_char(calendar_date, 'YYYYMMDD')::integer as date_id,
        calendar_date as full_date,
        year(calendar_date) as year,
        quarter(calendar_date) as quarter,
        month(calendar_date) as month,
        to_char(calendar_date, 'Month') as month_name,
        weekofyear(calendar_date) as week_of_year,
        dayofweek(calendar_date) as day_of_week,
        dayname(calendar_date) as day_name,
        dayofmonth(calendar_date) as day_of_month,
        dayofyear(calendar_date) as day_of_year,
        case when dayofweek(calendar_date) in (0, 6) then true else false 
            end as is_weekend,
        to_char(calendar_date, 'YYYY-MM') as year_month

    from date_spine
    where calendar_date <= dateadd('day', 730, current_date())

)

select * from final