{#
  By default dbt names custom schemas "<target_schema>_<custom_schema>" (e.g. gold_staging).
  This override uses the custom schema name exactly (staging, gold), so tables land in
  gridpulse.staging and gridpulse.gold, matching the rest of the lakehouse.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
