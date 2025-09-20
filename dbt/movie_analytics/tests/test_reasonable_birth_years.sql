-- Test: Birth year should be reasonable (not in the future, not too old)
{{ config(severity = 'warn') }}

select 
    person_id,
    primary_name,
    birth_year,
    'Birth year seems unreasonable' as warning_message
from {{ ref('stg_name_basics') }}
where birth_year is not null 
  and (birth_year > extract(year from current_date) 
       or birth_year < 1800)