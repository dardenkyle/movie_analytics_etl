-- Test: Rating values should be between 1 and 10
{{ config(severity = 'error') }}

select 
    title_id,
    average_rating,
    'Rating out of valid range (1-10)' as error_message
from {{ ref('stg_title_ratings') }}
where average_rating < 1.0 
   or average_rating > 10.0