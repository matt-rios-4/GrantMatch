-- Regla: fact_awards tiene exactamente una fila por award de slv_awards.
-- Si un JOIN con una dimensión duplicara filas, o el inner join con dim_award perdiera awards,
-- los conteos no coinciden y el test falla (devuelve una fila).
with s as (select count(*) as n from {{ ref('slv_awards') }}),
     f as (select count(*) as n, count(distinct award_key) as n_distinct from {{ ref('fact_awards') }})
select s.n as slv_awards, f.n as fact_rows, f.n_distinct as fact_distinct
from s, f
where s.n <> f.n or f.n <> f.n_distinct
