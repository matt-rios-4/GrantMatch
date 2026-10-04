{#
    SILVER · slv_work_awards
    GRAIN: una fila por award que financió un work (work_id + award_id).
    Es el enlace "esta publicación fue financiada por esta ayuda", la base de la etiqueta real
    del recomendador (investigador -> financiamiento obtenido).
    MATERIALIZACIÓN: incremental + delete+insert por work_id (hijas del work, igual que autorías).

    DECISIONES DE LIMPIEZA (ids W06 y W09):
      - Se descartan menciones sin award_id (W06): OpenAlex detectó un agradecimiento a un
        financiador pero no lo pudo emparejar con una ayuda concreta.
      - Un award repetido dentro del mismo work se deja una vez.
      - No se exige que el award exista en slv_awards: si solo se cargó una parte de los
        awards, el enlace se conserva aquí y Gold filtra a los que sí existen (W09 mide cuántos).
#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='work_id'
) }}

with works as (

    select work_id, awards, _loaded_at
    from {{ ref('slv_works') }}
    {% if is_incremental() %}
    where _loaded_at > {{ max_loaded_at() }}
    {% endif %}

),

flattened as (

    select
        w.work_id,
        {{ oa_id("a.value:id") }}                          as award_id,
        {{ oa_id("a.value:funder_id") }}                   as funder_id,
        nullif(trim(a.value:funder_award_id::varchar), '') as funder_award_id,
        a.index                                            as award_order,
        w._loaded_at
    from works w,
        lateral flatten(input => w.awards) a

)

select *
from flattened
where award_id is not null
qualify row_number() over (partition by work_id, award_id order by award_order) = 1
