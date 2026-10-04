{#
    Helpers de limpieza para OpenAlex.

    oa_id(expr): OpenAlex entrega los IDs como URL ("https://openalex.org/G5066037109",
    "https://openalex.org/subfields/1702", "https://orcid.org/0000-0002-..."). Nos quedamos con
    el último segmento para que la misma entidad tenga la misma llave en todas las tablas
    (un award la referencia como URL completa y un work como URL; sin normalizar, los JOIN fallan).

    clean_text(expr): recorta espacios, colapsa espacios repetidos, quita etiquetas HTML
    (los resúmenes de NSF traen <br/>, <p>, etc.) y convierte el texto vacío en NULL.
#}

{% macro oa_id(expr) -%}
    nullif(split_part(trim({{ expr }}::varchar), '/', -1), '')
{%- endmacro %}

{% macro clean_text(expr) -%}
    nullif(trim(regexp_replace(regexp_replace({{ expr }}::varchar, '<[^>]+>', ' '), '[[:space:]]+', ' ')), '')
{%- endmacro %}

{% macro max_loaded_at() -%}
    (select coalesce(max(_loaded_at), '1900-01-01'::timestamp_ltz) from {{ this }})
{%- endmacro %}

{% macro to_date_key(expr) -%}
    iff({{ expr }} between '{{ var("dim_date_start") }}'::date and '{{ var("dim_date_end") }}'::date,
        to_number(to_char({{ expr }}, 'YYYYMMDD')),
        -1)
{%- endmacro %}
