{#
    GOLD · dim_work
    GRAIN: una fila por publicación. title + abstract_text forman el perfil textual del
    investigador en el recomendador.
    MATERIALIZACIÓN: incremental + merge por work_key.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='work_key'
) }}

select
    w.work_id                                   as work_key,
    w.doi,
    w.title,
    w.abstract_text,
    w.has_abstract,
    w.publication_year,
    {{ to_date_key("w.publication_date") }}     as publication_date_key,
    w.work_type,
    w.language,
    w.cited_by_count,
    w.fwci,
    w.is_retracted,
    coalesce(t.topic_key, 'UNKNOWN')            as topic_key,
    w.authors_count,
    w._loaded_at
from {{ ref('slv_works') }} w
left join {{ ref('dim_topic') }} t
    on t.topic_key = w.primary_topic_id
{% if is_incremental() %}
where w._loaded_at > {{ max_loaded_at() }}
{% endif %}
