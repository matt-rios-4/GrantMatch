{#
    SILVER · slv_awards_history
    GRAIN: una fila por versión de cada award (award_id + updated_at).
    MATERIALIZACIÓN: incremental + append (default de la carpeta). Una versión de un award es un
    evento inmutable: nunca se actualiza, solo llegan versiones nuevas.

    Por qué "versión" y no "award": en OpenAlex un registro que cambia sale de su partición
    updated_date vieja y aparece en la nueva. Cada carga incremental de Kestra puede traer otra
    versión de un award que ya teníamos. Si el grain fuera "un award", el append duplicaría.
    slv_awards (estado actual) se queda con la última versión de cada uno.

    DECISIONES DE LIMPIEZA (métricas en analyses/data_quality_profile.sql, ids A01-A12):
      Consistencia
        - IDs: de URL a ID corto (oa_id) para que award, funder, topic, institution y work
          se unan por la misma llave en todas las tablas.
        - currency en mayúsculas; funding_type en minúsculas; texto sin espacios repetidos.
      Validez
        - amount <= 0 -> NULL y bandera is_amount_invalid (A06). Un monto cero o negativo no es un
          monto real; casi siempre significa "no informado". No se borra el award.
        - end_date < start_date (exacta o derivada del año) -> end_date = NULL y bandera
          has_inconsistent_dates (A08).
        - start_date ausente pero start_year presente -> 1 de enero de ese año, con bandera
          is_start_date_from_year, para no perder la dimensión de tiempo.
      Completitud
        - amount NULL se conserva como NULL (A05). No se imputa: los financiadores que no publican
          montos no lo hacen al azar (probable MNAR), e imputar con media inventaría dinero.
        - description NULL se conserva (A04). El award es válido, pero sin texto no se puede
          vectorizar: se marca con has_description y la OBT decide si lo usa.
        - Se descartan filas sin award_id (A01): sin llave no se pueden unir ni deduplicar.
      Unicidad
        - Si Kestra recarga el mismo archivo (reintento o backfill repetido) llegan copias exactas
          de (award_id, updated_at) (A02). Se queda una sola, la de la carga más reciente.
      Formato
        - description sin etiquetas HTML (A10): los resúmenes de NSF traen <br/> y <p>, que
          ensucian los embeddings.
#}

with source as (

    select raw, _source_file, _loaded_at
    from {{ source('bronze', 'raw_openalex_awards') }}
    {% if is_incremental() %}
    where _loaded_at > {{ max_loaded_at() }}
    {% endif %}

),

typed as (

    select
        {{ oa_id("raw:id") }}                                       as award_id,
        try_to_timestamp_ntz(raw:updated_date::varchar)             as updated_at,
        try_to_timestamp_ntz(raw:created_date::varchar)             as created_at,

        {{ clean_text("raw:display_name") }}                        as award_title,
        {{ clean_text("raw:description") }}                         as award_description,
        nullif(trim(raw:funder_award_id::varchar), '')              as funder_award_id,
        nullif(trim(raw:funder_scheme::varchar), '')                as funder_scheme,
        nullif(lower(trim(raw:funding_type::varchar)), '')          as funding_type,

        {{ oa_id("raw:funder:id") }}                                as funder_id,
        {{ clean_text("raw:funder:display_name") }}                 as funder_name,

        try_cast(raw:amount::varchar as number(18, 2))              as amount_raw,
        nullif(upper(trim(raw:currency::varchar)), '')              as currency,

        try_to_date(raw:start_date::varchar)                        as start_date_raw,
        try_cast(raw:start_year::varchar as integer)                as start_year,
        -- Fecha de inicio final: la exacta o, si falta, el 1 de enero de start_year.
        coalesce(start_date_raw,
                 iff(start_year between 1900 and 2100, date_from_parts(start_year, 1, 1), null))
                                                                    as start_date,
        try_to_date(raw:end_date::varchar)                          as end_date_raw,
        try_cast(raw:end_year::varchar as integer)                  as end_year,

        {{ oa_id("raw:primary_topic:id") }}                         as primary_topic_id,
        raw:institution_awarded                                     as institutions_awarded,
        {{ oa_id("raw:institution_awarded[0]:id") }}                as lead_institution_id,
        coalesce(array_size(raw:institution_awarded), 0)            as institutions_count,

        raw:funded_outputs                                          as funded_outputs,
        coalesce(try_cast(raw:funded_outputs_count::varchar as integer),
                 array_size(raw:funded_outputs), 0)                 as funded_outputs_count,

        nullif(lower(trim(raw:doi::varchar)), '')                   as doi,
        nullif(trim(raw:landing_page_url::varchar), '')             as landing_page_url,
        nullif(trim(raw:provenance::varchar), '')                   as provenance,

        _source_file,
        _loaded_at

    from source

),

cleaned as (

    select
        award_id,
        updated_at,
        created_at,
        award_title,
        award_description,
        award_description is not null                               as has_description,
        funder_award_id,
        funder_scheme,
        funding_type,
        funder_id,
        funder_name,

        iff(amount_raw > 0, amount_raw, null)                       as amount,
        coalesce(amount_raw <= 0, false)                            as is_amount_invalid,
        currency,
        iff(currency = 'USD' and amount_raw > 0, amount_raw, null)  as amount_usd,

        start_date,
        coalesce(start_date_raw is null and start_year between 1900 and 2100, false)
                                                                    as is_start_date_from_year,
        start_year,
        -- Contra la fecha de inicio final (exacta o derivada del año), no solo la exacta.
        iff(end_date_raw < start_date, null, end_date_raw)          as end_date,
        coalesce(end_date_raw < start_date, false)                  as has_inconsistent_dates,
        end_year,

        primary_topic_id,
        institutions_awarded,
        lead_institution_id,
        institutions_count,
        funded_outputs,
        funded_outputs_count,
        doi,
        landing_page_url,
        provenance,
        _source_file,
        _loaded_at

    from typed
    where award_id is not null

),

deduplicated as (

    select *
    from cleaned
    qualify row_number() over (
        partition by award_id, updated_at
        order by _loaded_at desc, _source_file desc
    ) = 1

)

select d.*
from deduplicated d
{% if is_incremental() %}
-- Idempotencia: si Kestra vuelve a cargar una versión que ya está en el historial, no se inserta.
where not exists (
    select 1
    from {{ this }} t
    where t.award_id = d.award_id
      and equal_null(t.updated_at, d.updated_at)
)
{% endif %}
