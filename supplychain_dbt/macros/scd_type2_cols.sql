{% macro scd_type2_cols() %}
    -- Standard SCD Type II tracking columns added to every dimension snapshot
    dbt_scd_id         as surrogate_key,
    dbt_valid_from     as valid_from,
    dbt_valid_to       as valid_to,
    case
        when dbt_valid_to is null then true
        else false
    end                as is_current,
    dbt_updated_at     as updated_at
{% endmacro %}