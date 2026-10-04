{#
    GOLD · dim_funder
    GRAIN: una fila por entidad financiadora, más 'UNKNOWN' para awards sin funder identificado.
    MATERIALIZACIÓN: table (catálogo pequeño y mutable).
#}
{{ config(materialized='table') }}

select
    funder_id           as funder_key,
    funder_name,
    country_code,
    is_global_south,
    ror_id,
    homepage_url,
    awards_count,
    works_count
from {{ ref('slv_funders') }}

union all

select 'UNKNOWN', 'Financiador desconocido', null, null, null, null, null, null
