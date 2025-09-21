{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with titles_base as (
    select * from {{ ref('stg_title_basics') }}
),

titles_enhanced as (
    select
        -- Primary key
        title_id,
        
        -- Basic information
        primary_title,
        original_title,
        title_type,
        
        -- Content classification
        is_adult,
        case 
            when is_adult then 'Adult Content'
            else 'General Audience'
        end as content_rating_category,
        
        -- Temporal information
        start_year,
        end_year,
        case 
            when start_year is not null then floor(start_year / 10) * 10
            else null 
        end as decade,
        case 
            when end_year is not null and start_year is not null 
            then end_year - start_year + 1
            else null 
        end as series_duration_years,
        
        -- Runtime information
        runtime_minutes,
        case 
            when runtime_minutes is null then 'Unknown'
            when runtime_minutes <= 30 then 'Short (≤30 min)'
            when runtime_minutes <= 90 then 'Standard (31-90 min)'  
            when runtime_minutes <= 180 then 'Long (91-180 min)'
            else 'Extended (>180 min)'
        end as runtime_category,
        
        -- Title type categorization
        case 
            when title_type in ('movie', 'tvMovie') then 'Movie'
            when title_type in ('tvSeries', 'tvMiniSeries') then 'TV Series'
            when title_type in ('tvEpisode') then 'TV Episode'
            when title_type in ('short', 'tvShort') then 'Short Film'
            when title_type in ('tvSpecial') then 'TV Special'
            when title_type in ('video') then 'Video'
            when title_type in ('videoGame') then 'Video Game'
            else 'Other'
        end as content_category,
        
        -- Genre information (raw for now, will enhance later)
        genres_raw,
        case 
            when genres_raw is not null then 
                array_length(string_to_array(genres_raw, ','), 1)
            else 0 
        end as genre_count,
        
        -- Derived flags
        case 
            when title_type in ('tvSeries', 'tvMiniSeries') and end_year is null 
            then true 
            else false 
        end as is_ongoing_series,
        
        case 
            when start_year >= extract(year from current_date) - 5 
            then true 
            else false 
        end as is_recent_title,
        
        -- Title length analysis
        length(primary_title) as title_character_count,
        case 
            when length(primary_title) <= 20 then 'Short Title'
            when length(primary_title) <= 50 then 'Medium Title'
            else 'Long Title'
        end as title_length_category
        
    from titles_base
)

select * from titles_enhanced