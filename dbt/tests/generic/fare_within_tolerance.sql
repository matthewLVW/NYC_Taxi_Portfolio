{% test fare_within_tolerance(model, column_name=None, actual_col=None, expected_col=None, tolerance=0.5, **kwargs) %}

{#-
  Supports both styles:
   - model test:
       - fare_within_tolerance:
           arguments:
             actual_col: manual_total
             expected_col: total_amount
             tolerance: 0.50
   - column test (if you ever use it that way):
       tests:
         - fare_within_tolerance:
             arguments:
               expected_col: total_amount
               tolerance: 0.50
     where dbt passes `column_name` as the actual_col.
-#}

{# resolve actual column #}
{% set actual = actual_col or column_name %}
{% if not actual %}
  {{ exceptions.raise_compiler_error("fare_within_tolerance: 'actual_col' (or 'column_name') is required") }}
{% endif %}
{% if not expected_col %}
  {{ exceptions.raise_compiler_error("fare_within_tolerance: 'expected_col' is required") }}
{% endif %}

-- Fail any rows whose absolute difference exceeds tolerance
select *
from {{ model }}
where abs({{ actual }} - {{ expected_col }}) > {{ tolerance }}

{% endtest %}
