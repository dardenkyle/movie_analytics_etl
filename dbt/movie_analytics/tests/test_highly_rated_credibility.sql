-- Test that highly rated movies have reasonable vote counts
-- Ensures that titles with excellent ratings (8.0+) have sufficient votes to be credible

select 
    title_id,
    average_rating,
    num_votes,
    rating_category
from {{ ref('fact_ratings') }}
where 
    average_rating >= 8.0 
    and num_votes < 100  -- Flag titles with high ratings but very few votes
    and average_rating is not null
    and num_votes is not null