{#
    GOLD · brg_work_author (tabla puente)
    GRAIN: una fila por autor dentro de una publicación (work_key + author_key).
    Resuelve la relación muchos a muchos entre dim_work y dim_author. Con fact_award_works
    permite llegar a la etiqueta del recomendador: investigador -> award que financió su trabajo.
    MATERIALIZACIÓN: incremental + delete+insert por work_key (hijas del work).
#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='work_key'
) }}

select
    a.work_id                       as work_key,
    a.author_id                     as author_key,
    a.author_position,
    a.is_corresponding,
    a.institution_id,
    a.institution_country_code,
    a._loaded_at
from {{ ref('slv_work_authorships') }} a
-- Solo autorías cuyo work y autor existen en Gold (integridad referencial estricta).
inner join {{ ref('dim_work') }} w   on w.work_key = a.work_id
inner join {{ ref('dim_author') }} au on au.author_key = a.author_id
{% if is_incremental() %}
where a._loaded_at > {{ max_loaded_at() }}
{% endif %}
