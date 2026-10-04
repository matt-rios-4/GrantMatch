{#
    GOLD · dim_institution
    GRAIN: una fila por institución que recibió al menos un award, más 'UNKNOWN'.
    Se construye desde institution_awarded de los awards (no se carga la entidad institutions
    completa: solo hacen falta las que reciben financiamiento).
    MATERIALIZACIÓN: incremental + merge por institution_key: solo procesa awards nuevos.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='institution_key'
) }}

with awards as (

    select institutions_awarded, _loaded_at
    from {{ ref('slv_awards') }}
    {% if is_incremental() %}
    where _loaded_at > {{ max_loaded_at() }}
    {% endif %}

),

institutions as (

    select
        {{ oa_id("i.value:id") }}                                   as institution_key,
        {{ clean_text("i.value:display_name") }}                    as institution_name,
        nullif(upper(trim(i.value:country_code::varchar)), '')      as country_code,
        nullif(lower(trim(i.value:type::varchar)), '')              as institution_type,
        {{ oa_id("i.value:ror") }}                                  as ror_id,
        a._loaded_at
    from awards a,
        lateral flatten(input => a.institutions_awarded) i

)

select *
from institutions
where institution_key is not null
qualify row_number() over (partition by institution_key order by _loaded_at desc) = 1

{% if not is_incremental() %}
union all
select 'UNKNOWN', 'Institución desconocida', null, null, null, '1900-01-01'::timestamp_ltz
{% endif %}
