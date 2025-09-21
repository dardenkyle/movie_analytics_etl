-- Test: Movie start years should be reasonable
{{ config(severity = 'warn') }}

select
    title_id,
    primary_title,
    start_year,
    'Start year seems unreasonable for a movie/show' as warning_message
from {{ ref('stg_title_basics') }}
where start_year is not null
  and (start_year > extract(year from current_date) + 5
       or start_year < 1800)
