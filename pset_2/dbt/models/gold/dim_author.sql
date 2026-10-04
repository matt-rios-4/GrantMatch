{#
    GOLD · dim_author
    GRAIN: una fila por investigador (author_id de OpenAlex).
    El nombre puede variar entre publicaciones; se usa el de la autoría cargada más recientemente.
    MATERIALIZACIÓN: incremental + merge por author_key.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='author_key'
) }}

select
    author_id       as author_key,
    author_name,
    orcid,
    _loaded_at
from {{ ref('slv_work_authorships') }}
{% if is_incremental() %}
where _loaded_at > {{ max_loaded_at() }}
{% endif %}
qualify row_number() over (
    partition by author_id
    order by _loaded_at desc, (orcid is null), author_name
) = 1
