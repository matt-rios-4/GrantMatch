{#
    GOLD · fact_award_works (hecho sin métricas / factless)
    GRAIN: una fila por publicación financiada por un award (award_key + work_key).

    Es la "verdad de terreno" del recomendador: si un work de un autor fue financiado por un
    award, ese perfil fue pertinente para ese financiamiento. Con brg_work_author se obtiene
    el par investigador -> award que la OBT de Spark usa como etiqueta positiva.

    No lleva el monto del award: en este grain se repetiría en cada publicación y sumarlo
    inflaría el financiamiento. El monto se analiza en fact_awards.

    Solo entran enlaces cuyo award y work existen en Gold (integridad estricta). Los enlaces a
    awards no cargados se quedan en slv_work_awards y se miden en el perfil de calidad (W09).

    MATERIALIZACIÓN: incremental + delete+insert por work_key (los enlaces son hijos del work).
#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='work_key'
) }}

select
    wa.award_id                             as award_key,
    wa.work_id                              as work_key,
    fa.funder_key,
    w.topic_key,
    w.publication_date_key,
    1                                       as funded_work_count,
    wa._loaded_at
from {{ ref('slv_work_awards') }} wa
inner join {{ ref('dim_work') }} w      on w.work_key = wa.work_id
inner join {{ ref('fact_awards') }} fa   on fa.award_key = wa.award_id
{% if is_incremental() %}
where wa._loaded_at > {{ max_loaded_at() }}
{% endif %}
