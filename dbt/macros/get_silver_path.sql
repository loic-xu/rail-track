{% macro get_silver_path() %}
  {%- if env_var('DBT_SILVER_PATH', '') -%}
    '{{ env_var("DBT_SILVER_PATH") }}'
  {%- elif env_var('PWD', '').endswith('/dbt') -%}
    '{{ env_var("PWD") }}/../data/silver/year=*/month=*/*.parquet'
  {%- else -%}
    '{{ env_var("PWD") }}/data/silver/year=*/month=*/*.parquet'
  {%- endif -%}
{% endmacro %}
