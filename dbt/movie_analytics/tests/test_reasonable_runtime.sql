-- Test: Runtime should be reasonable for movies/shows
{{ config(severity = 'warn') }}

select 
    title_id,
    primary_title,
    runtime_minutes,
    title_type,
    'Runtime seems unreasonable' as warning_message
from {{ ref('stg_title_basics') }}
where runtime_minutes is not null 
  and (runtime_minutes > 600  -- Over 10 hours seems excessive
       or runtime_minutes < 1) -- Less than 1 minute seems wrong