{% test positive_value(model, column_name) %}
-- Asserts that a numeric column contains only positive values (> 0).
select *
from {{ model }}
where {{ column_name }} <= 0
{% endtest %}


{% test not_negative(model, column_name) %}
-- Asserts that a numeric column contains no negative values (>= 0).
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}


{% test valid_delivery_status(model, column_name) %}
-- Asserts delivery_status is one of the three expected values.
select *
from {{ model }}
where {{ column_name }} not in ('ON_TIME', 'LATE', 'PENDING')
{% endtest %}


{% test valid_stock_level(model, column_name) %}
-- Asserts stock_level is one of the three expected values.
select *
from {{ model }}
where {{ column_name }} not in ('ADEQUATE', 'LOW_STOCK', 'OUT_OF_STOCK')
{% endtest %}