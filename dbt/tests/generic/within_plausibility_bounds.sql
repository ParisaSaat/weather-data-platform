{#
  Fails for rows whose value lies outside the seeded plausible range for its element.
  Usage:
    columns:
      - name: value_clean
        data_tests:
          - within_plausibility_bounds:
              arguments: {element_column: element}
#}
{% test within_plausibility_bounds(model, column_name, element_column='element') %}

select m.*, b.min_value, b.max_value
from {{ model }} as m
inner join {{ ref('element_plausibility_bounds') }} as b
    on b.element = m.{{ element_column }}
where m.{{ column_name }} is not null
  and (m.{{ column_name }} < b.min_value or m.{{ column_name }} > b.max_value)

{% endtest %}
