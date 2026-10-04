{#
    SILVER · slv_works
    GRAIN: una fila por work (su versión más reciente).
    MATERIALIZACIÓN: incremental + merge sobre work_id (estado actual; misma justificación que
    slv_awards).

    Reconstrucción del abstract: OpenAlex no publica el resumen como texto sino como índice
    invertido {"palabra": [posiciones]}. Se aplana y se vuelve a ordenar por posición para tener
    texto plano, que es la entrada de los embeddings del recomendador. Se acepta que el índice
    llegue como objeto o como texto JSON, según cómo lo haya escrito el Parquet.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='work_id'
) }}

with changed as (

    select *
    from {{ ref('slv_works_history') }}
    {% if is_incremental() %}
    where _loaded_at > {{ max_loaded_at() }}
    {% endif %}

),

latest as (

    select *
    from changed
    qualify row_number() over (
        partition by work_id
        order by updated_at desc nulls last, _loaded_at desc
    ) = 1

),

abstract_words as (

    select
        l.work_id,
        w.key                       as word,
        p.value::integer            as position
    from latest l,
        lateral flatten(input => iff(is_object(l.abstract_inverted_index),
                                     l.abstract_inverted_index,
                                     try_parse_json(l.abstract_inverted_index::varchar))) w,
        lateral flatten(input => w.value) p

),

abstracts as (

    select
        work_id,
        listagg(word, ' ') within group (order by position) as abstract_text
    from abstract_words
    group by work_id

),

final as (

    select
        l.work_id,
        l.updated_at,
        l.doi,
        l.title,
        l.has_title,
        a.abstract_text,
        a.abstract_text is not null      as has_abstract,
        l.publication_year,
        l.is_publication_year_invalid,
        l.publication_date,
        l.work_type,
        l.language,
        l.cited_by_count,
        l.fwci,
        l.is_retracted,
        l.primary_topic_id,
        l.authorships,
        l.authors_count,
        l.awards,
        l.awards_count,
        l._source_file,
        l._loaded_at
    from latest l
    left join abstracts a
        on a.work_id = l.work_id

)

select f.*
from final f
{% if is_incremental() %}
left join {{ this }} cur
    on cur.work_id = f.work_id
where cur.work_id is null
   or f.updated_at > cur.updated_at
   or cur.updated_at is null
{% endif %}
