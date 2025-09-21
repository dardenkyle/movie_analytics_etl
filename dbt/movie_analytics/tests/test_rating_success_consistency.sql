-- Test for popularity score consistency
-- Ensures popularity scores align with rating categories

select 
    title_id,
    average_rating,
    num_votes,
    popularity_score,
    rating_category,
    success_category
from {{ ref('fact_ratings') }}
where 
    rating_category = 'Outstanding (9.0+)'
    and success_category = 'Critical Failure'  -- This combination should not exist
    and average_rating is not null
    and popularity_score is not null