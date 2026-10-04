{#
    SILVER · slv_topics
    GRAIN: una fila por topic (versión más reciente), con su jerarquía desnormalizada.
    MATERIALIZACIÓN: table (catálogo de 4.516 filas; misma justificación que slv_funders).

    DECISIONES DE LIMPIEZA (ids T01-T02):
      - IDs de topic, subfield, field y domain de URL a ID corto.
      - Duplicados de topic_id entre cargas (T02): versión más reciente.
      - Se descartan filas sin topic_id (T01).
#}
{{ config(materialized='table') }}

with source as (

    select raw, _loaded_at
    from {{ source('bronze', 'raw_openalex_topics') }}

),

typed as (

    select
        {{ oa_id("raw:id") }}                               as topic_id,
        {{ clean_text("raw:display_name") }}                as topic_name,
        {{ clean_text("raw:description") }}                 as topic_description,
        raw:keywords                                        as keywords,
        {{ oa_id("raw:subfield:id") }}                      as subfield_id,
        {{ clean_text("raw:subfield:display_name") }}       as subfield_name,
        {{ oa_id("raw:field:id") }}                         as field_id,
        {{ clean_text("raw:field:display_name") }}          as field_name,
        {{ oa_id("raw:domain:id") }}                        as domain_id,
        {{ clean_text("raw:domain:display_name") }}         as domain_name,
        try_cast(raw:works_count::varchar as integer)       as works_count,
        try_to_timestamp_ntz(raw:updated_date::varchar)     as updated_at,
        _loaded_at
    from source

)

select *
from typed
where topic_id is not null
qualify row_number() over (
    partition by topic_id
    order by updated_at desc nulls last, _loaded_at desc
) = 1
