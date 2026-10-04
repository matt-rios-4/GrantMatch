{#
    SILVER · slv_works_history
    GRAIN: una fila por versión de cada work (work_id + updated_at).
    MATERIALIZACIÓN: incremental + append (default de la carpeta). Mismo razonamiento que
    slv_awards_history: una versión es inmutable; el estado actual está en slv_works.

    Se conservan como VARIANT las listas que se aplanan aguas abajo:
      authorships            -> slv_work_authorships
      awards                 -> slv_work_awards
      abstract_inverted_index -> se reconstruye como texto en slv_works

    DECISIONES DE LIMPIEZA (métricas en analyses/data_quality_profile.sql, ids W01-W09):
      Consistencia
        - IDs de URL a ID corto (oa_id); doi en minúsculas; work_type en minúsculas.
        - title: se usa title y, si falta, display_name (son el mismo dato en OpenAlex).
      Validez
        - publication_year fuera de [min_publication_year, año actual + 1] -> NULL (W05).
          Son errores de captura (años 0, 9999); se conserva el work.
        - cited_by_count negativo -> NULL (no puede haber citas negativas).
      Completitud
        - Se descartan filas sin work_id (W01).
        - title y abstract nulos se conservan con banderas (W03, W04): el work sigue siendo
          una publicación financiada válida aunque no tenga texto.
      Unicidad
        - Copias exactas de (work_id, updated_at) por recargas de Kestra: se queda una (W02).
#}

with source as (

    select raw, _source_file, _loaded_at
    from {{ source('bronze', 'raw_openalex_works') }}
    {% if is_incremental() %}
    where _loaded_at > {{ max_loaded_at() }}
    {% endif %}

),

typed as (

    select
        {{ oa_id("raw:id") }}                                       as work_id,
        try_to_timestamp_ntz(raw:updated_date::varchar)             as updated_at,
        nullif(lower(trim(raw:doi::varchar)), '')                   as doi,
        coalesce({{ clean_text("raw:title") }},
                 {{ clean_text("raw:display_name") }})              as title,
        try_cast(raw:publication_year::varchar as integer)          as publication_year_raw,
        try_to_date(raw:publication_date::varchar)                  as publication_date,
        nullif(lower(trim(raw:type::varchar)), '')                  as work_type,
        nullif(lower(trim(raw:language::varchar)), '')              as language,
        try_cast(raw:cited_by_count::varchar as integer)            as cited_by_count_raw,
        try_cast(raw:fwci::varchar as float)                        as fwci,
        coalesce(try_cast(raw:is_retracted::varchar as boolean), false) as is_retracted,
        {{ oa_id("raw:primary_topic:id") }}                         as primary_topic_id,
        raw:abstract_inverted_index                                 as abstract_inverted_index,
        raw:authorships                                             as authorships,
        coalesce(array_size(raw:authorships), 0)                    as authors_count,
        raw:awards                                                  as awards,
        coalesce(array_size(raw:awards), 0)                         as awards_count,
        _source_file,
        _loaded_at
    from source

),

cleaned as (

    select
        work_id,
        updated_at,
        doi,
        title,
        title is not null                                           as has_title,
        iff(publication_year_raw between {{ var('min_publication_year') }}
                                     and year(current_date()) + 1,
            publication_year_raw, null)                             as publication_year,
        coalesce(not (publication_year_raw between {{ var('min_publication_year') }}
                                               and year(current_date()) + 1), false)
                                                                    as is_publication_year_invalid,
        publication_date,
        work_type,
        language,
        iff(cited_by_count_raw >= 0, cited_by_count_raw, null)      as cited_by_count,
        fwci,
        is_retracted,
        primary_topic_id,
        abstract_inverted_index,
        authorships,
        authors_count,
        awards,
        awards_count,
        _source_file,
        _loaded_at
    from typed
    where work_id is not null

),

deduplicated as (

    select *
    from cleaned
    qualify row_number() over (
        partition by work_id, updated_at
        order by _loaded_at desc, _source_file desc
    ) = 1

)

select d.*
from deduplicated d
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t
    where t.work_id = d.work_id
      and equal_null(t.updated_at, d.updated_at)
)
{% endif %}
