{#
    GOLD · dim_date
    GRAIN: una fila por día calendario entre dim_date_start y dim_date_end, más la fila -1
    ("fecha desconocida") para awards o works sin fecha válida.
    MATERIALIZACIÓN: table (~31 mil filas, generada; no viene de un evento).
#}
{{ config(materialized='table') }}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('dim_date_start') ~ "' as date)",
        end_date="dateadd(day, 1, cast('" ~ var('dim_date_end') ~ "' as date))"
    ) }}

),

days as (

    select
        to_number(to_char(date_day, 'YYYYMMDD'))  as date_key,
        date_day::date                            as full_date,
        year(date_day)                            as year,
        quarter(date_day)                         as quarter,
        month(date_day)                           as month,
        monthname(date_day)                       as month_name,
        day(date_day)                             as day_of_month,
        dayofweekiso(date_day)                    as day_of_week_iso,
        dayofweekiso(date_day) in (6, 7)          as is_weekend,
        -- Año fiscal federal de EE. UU. (octubre-septiembre): así planifica NSF sus convocatorias.
        iff(month(date_day) >= 10, year(date_day) + 1, year(date_day)) as us_fiscal_year
    from spine

)

select * from days
union all
select -1, null, null, null, null, 'Desconocido', null, null, null, null
