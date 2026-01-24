{% macro try_to_date(expr) %}
  {% if target.type == "snowflake" %}
    TRY_TO_DATE({{ expr }})
  {% else %}
    TRY_CAST({{ expr }} AS DATE)
  {% endif %}
{% endmacro %}

{% macro try_to_float(expr) %}
  {% if target.type == "snowflake" %}
    TRY_TO_DOUBLE({{ expr }})
  {% else %}
    TRY_CAST({{ expr }} AS DOUBLE)
  {% endif %}
{% endmacro %}

{% macro hash_record(expr) %}
  {% if target.type == "snowflake" %}
    SHA2({{ expr }}, 256)
  {% else %}
    MD5({{ expr }})
  {% endif %}
{% endmacro %}
