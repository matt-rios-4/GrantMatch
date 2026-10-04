{#
    Sin este override, dbt concatena el esquema del perfil con el del modelo:
    profiles.yml usa SILVER por defecto, así que +schema: gold terminaría en SILVER_GOLD.
    Con el override, cada modelo cae exactamente en PSET2_DB.SILVER o PSET2_DB.GOLD,
    que son los esquemas que crea docs/snowflake_setup.sql.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
