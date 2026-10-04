{#
    GOLD · dim_award
    GRAIN: una fila por award. Guarda los atributos descriptivos (título, texto, tipo de ayuda);
    las métricas y llaves foráneas viven en fact_awards. award_description es el texto que se
    vectoriza para el recomendador.
    MATERIALIZACIÓN: incremental + merge por award_key (estado actual del award).
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='award_key'
) }}

select
    award_id            as award_key,
    funder_award_id,
    award_title,
    award_description,
    has_description,
    funder_scheme,
    funding_type,
    doi,
    landing_page_url,
    provenance,
    _loaded_at
from {{ ref('slv_awards') }}
{% if is_incremental() %}
where _loaded_at > {{ max_loaded_at() }}
{% endif %}
