-- Test for reasonable career spans
-- Ensures people don't have impossibly long careers (over 80 years active)

select 
    person_id,
    primary_name,
    birth_year,
    death_year,
    potential_career_span_years,
    age_or_current_age
from {{ ref('dim_people') }}
where 
    potential_career_span_years > 80  -- Flag careers longer than 80 years
    and potential_career_span_years is not null