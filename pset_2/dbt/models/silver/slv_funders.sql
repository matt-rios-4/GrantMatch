{#
    SILVER · slv_funders
    GRAIN: una fila por funder (versión más reciente).
    MATERIALIZACIÓN: table. Excepción justificada al append: es un catálogo pequeño (~30 mil filas)
    y mutable (sus conteos de awards y works cambian en cada snapshot). Reconstruirlo cuesta
    segundos y garantiza una sola fila por funder sin lógica incremental.

    DECISIONES DE LIMPIEZA (ids F01-F03):
      - IDs y ROR de URL a ID corto (oa_id).
      - Duplicados de funder_id entre cargas (F02): se queda la versión con updated_at más reciente.
      - Se descartan filas sin funder_id (F01).
      - country_code en mayúsculas; conteos negativos -> NULL.
#}
{{ config(materialized='table') }}

with source as (

    select raw, _loaded_at
    from {{ source('bronze', 'raw_openalex_funders') }}

),

typed as (

    select
        {{ oa_id("raw:id") }}                                   as funder_id,
        {{ clean_text("raw:display_name") }}                    as funder_name,
        {{ clean_text("raw:description") }}                     as funder_description,
        nullif(upper(trim(raw:country_code::varchar)), '')      as country_code,
        try_cast(raw:is_global_south::varchar as boolean)       as is_global_south,
        {{ oa_id("raw:ids:ror") }}                              as ror_id,
        nullif(trim(raw:homepage_url::varchar), '')             as homepage_url,
        iff(try_cast(raw:awards_count::varchar as integer) >= 0,
            try_cast(raw:awards_count::varchar as integer), null) as awards_count,
        iff(try_cast(raw:works_count::varchar as integer) >= 0,
            try_cast(raw:works_count::varchar as integer), null)  as works_count,
        try_to_timestamp_ntz(raw:updated_date::varchar)         as updated_at,
        _loaded_at
    from source

)

select *
from typed
where funder_id is not null
qualify row_number() over (
    partition by funder_id
    order by updated_at desc nulls last, _loaded_at desc
) = 1
