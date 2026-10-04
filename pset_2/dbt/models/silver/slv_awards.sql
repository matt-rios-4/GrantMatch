{#
    SILVER · slv_awards
    GRAIN: una fila por award (su versión más reciente).
    MATERIALIZACIÓN: incremental + merge sobre award_id. Excepción justificada al append:
    esta tabla representa el ESTADO ACTUAL, no un evento. Si un award cambia (por ejemplo, se le
    enlazan más publicaciones), con append quedarían dos filas para el mismo award. El merge
    solo toca los awards que llegaron en la última carga; no reconstruye la tabla completa.
    El historial inmutable vive en slv_awards_history (append).
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='award_id'
) }}

with changed as (

    select *
    from {{ ref('slv_awards_history') }}
    {% if is_incremental() %}
    where _loaded_at > {{ max_loaded_at() }}
    {% endif %}

),

latest as (

    select *
    from changed
    qualify row_number() over (
        partition by award_id
        order by updated_at desc nulls last, _loaded_at desc
    ) = 1

)

select l.*
from latest l
{% if is_incremental() %}
-- Nunca reemplazar una versión más nueva por una más vieja (pasa si un backfill
-- carga particiones antiguas después de las recientes).
left join {{ this }} cur
    on cur.award_id = l.award_id
where cur.award_id is null
   or l.updated_at > cur.updated_at
   or cur.updated_at is null
{% endif %}
