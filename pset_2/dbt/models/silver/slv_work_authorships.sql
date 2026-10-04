{#
    SILVER · slv_work_authorships
    GRAIN: una fila por autor dentro de un work (work_id + author_id).
    MATERIALIZACIÓN: incremental + delete+insert por work_id. Las autorías son hijas del work:
    cuando llega una versión nueva de un work se borran sus autorías anteriores y se insertan
    las nuevas, sin tocar los demás works.

    DECISIONES DE LIMPIEZA (ids W07-W08 en analyses/data_quality_profile.sql):
      - Se descartan autorías sin author_id (W07): OpenAlex no pudo identificar al autor y sin
        llave no se puede construir el perfil del investigador.
      - Un mismo autor repetido en un work (W08) se deja una vez, con su primera posición.
      - author_position se normaliza a minúsculas (first / middle / last).
      - Se toma la primera institución declarada como afiliación de esa autoría.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='work_id'
) }}

with works as (

    select work_id, authorships, _loaded_at
    from {{ ref('slv_works') }}
    {% if is_incremental() %}
    where _loaded_at > {{ max_loaded_at() }}
    {% endif %}

),

flattened as (

    select
        w.work_id,
        {{ oa_id("a.value:author:id") }}                       as author_id,
        {{ clean_text("a.value:author:display_name") }}        as author_name,
        {{ oa_id("a.value:author:orcid") }}                    as orcid,
        nullif(lower(trim(a.value:author_position::varchar)), '') as author_position,
        coalesce(try_cast(a.value:is_corresponding::varchar as boolean), false) as is_corresponding,
        {{ oa_id("a.value:institutions[0]:id") }}              as institution_id,
        {{ clean_text("a.value:institutions[0]:display_name") }} as institution_name,
        nullif(upper(trim(a.value:institutions[0]:country_code::varchar)), '') as institution_country_code,
        a.index                                                as author_order,
        w._loaded_at
    from works w,
        lateral flatten(input => w.authorships) a

)

select *
from flattened
where author_id is not null
qualify row_number() over (partition by work_id, author_id order by author_order) = 1
