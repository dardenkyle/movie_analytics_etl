{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with ratings_base as (
    select * from {{ ref('stg_title_ratings') }}
),

titles_info as (
    select
        title_id,
        title_type,
        content_category,
        start_year,
        decade,
        runtime_category,
        is_adult
    from {{ ref('dim_titles') }}
),

ratings_enhanced as (
    select
        -- Primary key and foreign key
        r.title_id,

        -- Basic rating information
        r.average_rating,
        r.num_votes,

        -- Title context from dimension
        t.title_type,
        t.content_category,
        t.start_year,
        t.decade,
        t.runtime_category,
        t.is_adult,

        -- Rating categories
        case
            when r.average_rating is null then 'Not Rated'
            when r.average_rating < 3.0 then 'Poor (< 3.0)'
            when r.average_rating < 5.0 then 'Below Average (3.0-4.9)'
            when r.average_rating < 7.0 then 'Average (5.0-6.9)'
            when r.average_rating < 8.0 then 'Good (7.0-7.9)'
            when r.average_rating < 9.0 then 'Excellent (8.0-8.9)'
            else 'Outstanding (9.0+)'
        end as rating_category,

        -- Vote volume categories
        case
            when r.num_votes is null then 'No Votes'
            when r.num_votes < 100 then 'Minimal (< 100)'
            when r.num_votes < 1000 then 'Low (100-999)'
            when r.num_votes < 10000 then 'Moderate (1K-9K)'
            when r.num_votes < 100000 then 'High (10K-99K)'
            when r.num_votes < 1000000 then 'Very High (100K-999K)'
            else 'Massive (1M+)'
        end as vote_volume_category,

        -- Statistical measures
        case
            when r.num_votes >= 1000 then true
            else false
        end as is_statistically_significant,

        case
            when r.average_rating >= 8.0 and r.num_votes >= 10000 then true
            else false
        end as is_highly_rated_popular,

        case
            when r.average_rating >= 7.0 and r.num_votes >= 100000 then true
            else false
        end as is_mainstream_success,

        -- Popularity score (weighted rating considering vote volume)
        case
            when r.average_rating is not null and r.num_votes is not null then
                r.average_rating * log(greatest(r.num_votes, 1))
            else null
        end as popularity_score,

        -- Rating confidence level
        case
            when r.num_votes is null or r.num_votes = 0 then 'No Confidence'
            when r.num_votes < 10 then 'Very Low Confidence'
            when r.num_votes < 100 then 'Low Confidence'
            when r.num_votes < 1000 then 'Medium Confidence'
            when r.num_votes < 10000 then 'High Confidence'
            else 'Very High Confidence'
        end as rating_confidence,

        -- Percentile rankings (approximate)
        case
            when r.average_rating is null then null
            when r.average_rating >= 8.5 then 'Top 1%'
            when r.average_rating >= 8.0 then 'Top 5%'
            when r.average_rating >= 7.5 then 'Top 10%'
            when r.average_rating >= 7.0 then 'Top 25%'
            when r.average_rating >= 6.0 then 'Top 50%'
            else 'Bottom 50%'
        end as rating_percentile_tier,

        -- Content analysis flags
        case
            when r.average_rating >= 8.0 then 'Critical Acclaim'
            when r.average_rating >= 7.0 and r.num_votes >= 50000 then 'Popular Success'
            when r.average_rating >= 6.0 and r.num_votes >= 10000 then 'Commercial Success'
            when r.average_rating < 4.0 and r.num_votes >= 5000 then 'Critical Failure'
            when r.num_votes >= 100000 then 'High Visibility'
            else 'Standard Release'
        end as success_category,

        -- Voting engagement rate (subjective measure)
        case
            when r.num_votes is null or r.num_votes = 0 then 0
            when t.start_year is not null then
                r.num_votes::float / greatest(extract(year from current_date) - t.start_year + 1, 1)
            else r.num_votes::float
        end as annual_vote_rate,

        -- Quality vs popularity quadrant
        case
            when r.average_rating >= 7.0 and r.num_votes >= 10000 then 'High Quality, High Popularity'
            when r.average_rating >= 7.0 and r.num_votes < 10000 then 'High Quality, Low Popularity'
            when r.average_rating < 7.0 and r.num_votes >= 10000 then 'Low Quality, High Popularity'
            else 'Low Quality, Low Popularity'
        end as quality_popularity_quadrant

    from ratings_base r
    left join titles_info t on r.title_id = t.title_id
)

select * from ratings_enhanced
