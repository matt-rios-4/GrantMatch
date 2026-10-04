{#
    GOLD · dim_topic
    GRAIN: una fila por topic, con la jerarquía domain > field > subfield > topic desnormalizada
    (star, no snowflake: un solo JOIN para filtrar por área del conocimiento).
    Incluye 'UNKNOWN' para awards o works sin tema asignado.
    MATERIALIZACIÓN: table (4.516 filas).
#}
{{ config(materialized='table') }}

select
    topic_id        as topic_key,
    topic_name,
    topic_description,
    keywords,
    subfield_id,
    subfield_name,
    field_id,
    field_name,
    domain_id,
    domain_name
from {{ ref('slv_topics') }}

union all

select 'UNKNOWN', 'Tema desconocido', null, null, null, null, null, null, null, null
