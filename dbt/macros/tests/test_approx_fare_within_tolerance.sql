{% test fare_within_tolerance(model, actual_col, expected_col, tolerance) %}
-- Passes if all UNFLAGGED rows are within tolerance.
-- Rows explicitly flagged (qa_is_fare_mismatch = true) are excluded from failure set.
select *
from {{ model }}
where
  {{ actual_col }} is not null
  and {{ expected_col }} is not null
  and abs(cast({{ actual_col }} as double) - cast({{ expected_col }} as double)) > {{ tolerance }}
  and coalesce(qa_is_fare_mismatch, false) = false
{% endtest %}
