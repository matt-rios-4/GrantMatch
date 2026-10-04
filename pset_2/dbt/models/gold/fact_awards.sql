{#
    GOLD · fact_awards (tabla de hechos principal)
    GRAIN: una fila por award (ayuda o subvención otorgada), en su versión más reciente.

    Responde preguntas como: ¿cuánto financia cada funder por área del conocimiento y por año?,
    ¿qué instituciones reciben más ayudas?, ¿cuánto duran las ayudas de cada tipo?

    Llaves foráneas (todas NOT NULL; lo que no se puede resolver va al miembro 'UNKNOWN' / -1):
      award_key -> dim_award · funder_key -> dim_funder · topic_key -> dim_topic
      institution_key -> dim_institution (institución principal) · start/end_date_key -> dim_date
    Métricas:
      amount, amount_usd (aditiva solo en USD), duration_days, funded_outputs_count,
      institutions_count, award_count (= 1, para contar ayudas al agregar).

    MATERIALIZACIÓN: incremental + merge por award_key. Un award puede actualizarse (por ejemplo,
    al enlazarse más publicaciones); con append quedaría duplicado y rompería el grain.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='award_key'
) }}

with awards as (

    select *
    from {{ ref('slv_awards') }}
    {% if is_incremental() %}
    where _loaded_at > {{ max_loaded_at() }}
    {% endif %}

)

select
    a.award_id                                          as award_key,
    coalesce(f.funder_key, 'UNKNOWN')                   as funder_key,
    coalesce(t.topic_key, 'UNKNOWN')                    as topic_key,
    coalesce(i.institution_key, 'UNKNOWN')              as institution_key,
    {{ to_date_key("a.start_date") }}                   as start_date_key,
    {{ to_date_key("a.end_date") }}                     as end_date_key,

    a.amount,
    a.currency,
    a.amount_usd,
    iff(a.end_date is not null and a.start_date is not null,
        datediff('day', a.start_date, a.end_date), null) as duration_days,
    a.funded_outputs_count,
    a.institutions_count,
    1                                                   as award_count,

    a.is_amount_invalid,
    a.is_start_date_from_year,
    a.has_inconsistent_dates,
    a._loaded_at

from awards a
inner join {{ ref('dim_award') }} da        on da.award_key = a.award_id
left join {{ ref('dim_funder') }} f         on f.funder_key = a.funder_id
left join {{ ref('dim_topic') }} t          on t.topic_key = a.primary_topic_id
left join {{ ref('dim_institution') }} i    on i.institution_key = a.lead_institution_id
