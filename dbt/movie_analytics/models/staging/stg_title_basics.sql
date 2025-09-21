{{ config(
    materialized = 'view',
    schema = 'staging'
) }} with source as (
    select
        *
    from
        {{ source('raw', 'title_basics') }}
),
cleaned as (
    select
        -- Primary key
        tconst as title_id,
        -- Basic title information
        titletype as title_type,
        primarytitle as primary_title,
        originaltitle as original_title,
        -- Content flags
        case
            when isadult = '1' then true
            when isadult = '0' then false
            else null
        end as is_adult,
        -- Years - convert \N to null and cast to integer
        case
            when startyear = '\\N' then null
            else startyear :: integer
        end as start_year,
        case
            when endyear = '\\N' then null
            else endyear :: integer
        end as end_year,
        -- Runtime - convert \N to null and cast to integer
        case
            when runtimeminutes = '\\N' then null
            else runtimeminutes :: integer
        end as runtime_minutes,
        -- Genres - keep as text for now, will split later
        case
            when genres = '\\N' then null
            else genres
        end as genres_raw
    from
        source
    -- Filter out records with invalid title types
    where
        titletype in (
            'movie', 'short', 'tvEpisode', 'tvMiniSeries', 'tvMovie',
            'tvSeries', 'tvShort', 'tvSpecial', 'video', 'videoGame'
        )
)
select
    *
from
    cleaned
