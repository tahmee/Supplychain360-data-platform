{% macro generate_sales_union() %}
{#
  Dynamically unions all sales_YYYY_MM_DD tables in the RAW schema
  into a single relation the stg_sales model can query as source('raw_postgres','sales').

  Run this macro once before dbt run to create/replace the union view:
    dbt run-operation generate_sales_union

  The resulting view RAW.SALES is what sources.yml points to.
#}

{% set query %}
    select table_name
    from information_schema.tables
    where table_schema = 'RAW'
      and table_name ilike 'SALES_%'
      and table_type = 'BASE TABLE'
    order by table_name
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
    {% set table_names = results.columns[0].values() %}

    {% if table_names | length == 0 %}
        {{ log("No sales tables found – skipping view creation.", info=True) }}
    {% else %}
        {% set union_sql %}
            create or replace view {{ target.database }}.RAW.SALES as
            {% for tbl in table_names %}
                select * from {{ target.database }}.RAW."{{ tbl }}"
                {% if not loop.last %} union all {% endif %}
            {% endfor %}
        {% endset %}

        {% do run_query(union_sql) %}
        {{ log("Created RAW.SALES view over " ~ table_names | length ~ " table(s).", info=True) }}
    {% endif %}
{% endif %}

{% endmacro %}
