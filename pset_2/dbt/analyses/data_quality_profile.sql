-- ============================================================================================
-- Perfil de calidad de datos sobre BRONZE (antes de limpiar).
-- Produce la evidencia numérica de la Sección 3 del memo: cada fila es un problema con su
-- conteo y porcentaje. Los IDs (A01, W04, ...) son los que citan los comentarios de los modelos
-- Silver y la tabla de docs/memo_secciones_3_4.md.
--
-- Cómo correrlo:
--   docker compose exec dbt dbt compile --select data_quality_profile
--   y pegar target/compiled/pset2/analyses/data_quality_profile.sql en un worksheet de Snowflake.
-- Hace escaneos completos de Bronze: correrlo una vez, después de la carga, no en cada pipeline.
-- ============================================================================================

with aw as (
    select
        {{ oa_id("raw:id") }}                                   as id,
        raw:updated_date::varchar                               as updated_date,
        {{ clean_text("raw:description") }}                     as description,
        raw:description::varchar                                as description_raw,
        try_cast(raw:amount::varchar as number(18, 2))          as amount,
        nullif(upper(trim(raw:currency::varchar)), '')          as currency,
        try_to_date(raw:start_date::varchar)                    as start_date,
        try_to_date(raw:end_date::varchar)                      as end_date,
        raw:start_year                                          as start_year,
        {{ oa_id("raw:funder:id") }}                            as funder_id,
        {{ oa_id("raw:primary_topic:id") }}                     as primary_topic_id,
        coalesce(try_cast(raw:funded_outputs_count::varchar as integer),
                 array_size(raw:funded_outputs), 0)             as funded_outputs_count
    from {{ source('bronze', 'raw_openalex_awards') }}
),
fu as (
    select {{ oa_id("raw:id") }} as id, {{ clean_text("raw:display_name") }} as name
    from {{ source('bronze', 'raw_openalex_funders') }}
),
tp as (
    select {{ oa_id("raw:id") }} as id
    from {{ source('bronze', 'raw_openalex_topics') }}
),
wk as (
    select
        {{ oa_id("raw:id") }}                                   as id,
        raw:updated_date::varchar                               as updated_date,
        coalesce({{ clean_text("raw:title") }}, {{ clean_text("raw:display_name") }}) as title,
        raw:abstract_inverted_index                             as abstract_idx,
        try_cast(raw:publication_year::varchar as integer)      as publication_year,
        coalesce(try_cast(raw:is_retracted::varchar as boolean), false) as is_retracted,
        raw:authorships                                         as authorships,
        raw:awards                                              as awards
    from {{ source('bronze', 'raw_openalex_works') }}
),
wk_authors as (
    select w.id as work_id, {{ oa_id("a.value:author:id") }} as author_id
    from wk w, lateral flatten(input => w.authorships) a
),
wk_awards as (
    select w.id as work_id, {{ oa_id("a.value:id") }} as award_id
    from wk w, lateral flatten(input => w.awards) a
),

metrics as (

    -- ---------------------------------------------------------------- AWARDS
    select 'A00' as metric_id, 'awards' as entidad, '-' as dimension, 'Filas en Bronze' as problema,
           count(*) as n_afectados, count(*) as n_total from aw
    union all
    select 'A01', 'awards', 'Completitud', 'Sin award_id',
           count_if(id is null), count(*) from aw
    union all
    select 'A02', 'awards', 'Unicidad', 'Copias exactas de (award_id, updated_date) por recarga',
           count(*) - count(distinct id || '|' || coalesce(updated_date, '')), count(*) from aw where id is not null
    union all
    select 'A03', 'awards', 'Unicidad', 'Awards con más de una versión (updated_date distinta)',
           count_if(n_versions > 1), count(*)
    from (select id, count(distinct updated_date) as n_versions from aw where id is not null group by id)
    union all
    select 'A04', 'awards', 'Completitud', 'Sin description (no se puede vectorizar)',
           count_if(description is null), count(*) from aw
    union all
    select 'A05', 'awards', 'Completitud', 'Sin amount',
           count_if(amount is null), count(*) from aw
    union all
    select 'A06', 'awards', 'Validez', 'amount <= 0',
           count_if(amount <= 0), count_if(amount is not null) from aw
    union all
    select 'A07', 'awards', 'Consistencia', 'Monto en moneda distinta de USD',
           count_if(currency <> 'USD'), count_if(amount > 0) from aw where amount > 0
    union all
    select 'A08', 'awards', 'Validez', 'end_date anterior a start_date',
           count_if(end_date < start_date), count_if(end_date is not null and start_date is not null) from aw
    union all
    select 'A09', 'awards', 'Completitud', 'Sin start_date ni start_year',
           count_if(start_date is null and start_year is null), count(*) from aw
    union all
    select 'A10', 'awards', 'Consistencia', 'description con etiquetas HTML',
           count_if(regexp_like(description_raw, '.*<[^>]+>.*', 's')), count_if(description is not null) from aw
    union all
    select 'A11', 'awards', 'Completitud', 'Sin funder identificado',
           count_if(funder_id is null), count(*) from aw
    union all
    select 'A12', 'awards', 'Consistencia', 'funder_id que no existe en funders cargados',
           count_if(aw.funder_id is not null and fu.id is null), count_if(aw.funder_id is not null)
    from aw left join (select distinct id from fu) fu on fu.id = aw.funder_id
    union all
    select 'A13', 'awards', 'Completitud', 'Sin publicaciones enlazadas (funded_outputs = 0)',
           count_if(funded_outputs_count = 0), count(*) from aw
    union all
    select 'A14', 'awards', 'Completitud', 'Sin primary_topic (no se puede filtrar por dominio)',
           count_if(primary_topic_id is null), count(*) from aw

    -- ---------------------------------------------------------------- WORKS
    union all
    select 'W00', 'works', '-', 'Filas en Bronze', count(*), count(*) from wk
    union all
    select 'W01', 'works', 'Completitud', 'Sin work_id', count_if(id is null), count(*) from wk
    union all
    select 'W02', 'works', 'Unicidad', 'Copias exactas de (work_id, updated_date) por recarga',
           count(*) - count(distinct id || '|' || coalesce(updated_date, '')), count(*) from wk where id is not null
    union all
    select 'W03', 'works', 'Completitud', 'Sin título', count_if(title is null), count(*) from wk
    union all
    select 'W04', 'works', 'Completitud', 'Sin abstract', count_if(abstract_idx is null), count(*) from wk
    union all
    select 'W05', 'works', 'Validez', 'publication_year fuera de rango',
           count_if(publication_year < {{ var('min_publication_year') }}
                    or publication_year > year(current_date()) + 1),
           count_if(publication_year is not null) from wk
    union all
    select 'W06', 'works', 'Completitud', 'Mención de award sin award_id',
           count_if(award_id is null), count(*) from wk_awards
    union all
    select 'W07', 'works', 'Completitud', 'Autoría sin author_id',
           count_if(author_id is null), count(*) from wk_authors
    union all
    select 'W08', 'works', 'Unicidad', 'Autor repetido dentro del mismo work',
           count(*) - count(distinct work_id || '|' || author_id), count(*) from wk_authors where author_id is not null
    union all
    select 'W09', 'works', 'Consistencia', 'Enlace a un award que no está cargado',
           count_if(a.id is null), count(*)
    from wk_awards wa left join (select distinct id from aw) a on a.id = wa.award_id
    where wa.award_id is not null
    union all
    select 'W10', 'works', 'Validez', 'Publicaciones retractadas', count_if(is_retracted), count(*) from wk

    -- ---------------------------------------------------------------- FUNDERS / TOPICS
    union all
    select 'F01', 'funders', 'Completitud', 'Sin funder_id', count_if(id is null), count(*) from fu
    union all
    select 'F02', 'funders', 'Unicidad', 'funder_id repetido entre cargas',
           count(*) - count(distinct id), count(*) from fu where id is not null
    union all
    select 'F03', 'funders', 'Completitud', 'Sin nombre', count_if(name is null), count(*) from fu
    union all
    select 'T01', 'topics', 'Completitud', 'Sin topic_id', count_if(id is null), count(*) from tp
    union all
    select 'T02', 'topics', 'Unicidad', 'topic_id repetido entre cargas',
           count(*) - count(distinct id), count(*) from tp where id is not null
)

select
    metric_id,
    entidad,
    dimension,
    problema,
    n_afectados,
    n_total,
    round(100 * n_afectados / nullif(n_total, 0), 2) as pct
from metrics
order by metric_id
