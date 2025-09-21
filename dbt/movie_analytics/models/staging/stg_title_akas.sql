{{ config(
    materialized = 'view',
    schema = 'staging'
) }} with source as (
    select
        *
    from
        {{ source('raw', 'title_akas') }}
),
cleaned as (
    select
        -- Foreign key
        titleid as title_id,
        -- Ordering and title information
        case
            when ordering = '\\N' then null
            else ordering :: integer
        end as ordering,
        case
            when title = '\\N' then null
            else title
        end as alternative_title,
        -- Geographic information
        case
            when region = '\\N' then null
            else region
        end as region_code,
        case
            when language = '\\N' then null
            else language
        end as language_code,
        -- Comma-separated fields - keep as text for now
        case
            when types = '\\N' then null
            else types
        end as title_types_raw,
        case
            when attributes = '\\N' then null
            else attributes
        end as attributes_raw,
        -- Original title flag
        case
            when isoriginaltitle = '1' then true
            when isoriginaltitle = '0' then false
            else null
        end as is_original_title
    from
        source
    -- Only include alternative titles for titles that exist in title_basics
    where
        titleid in (
            select tconst
            from {{ source('raw', 'title_basics') }}
            where titletype in (
                'movie', 'short', 'tvEpisode', 'tvMiniSeries', 'tvMovie',
                'tvSeries', 'tvShort', 'tvSpecial', 'video', 'videoGame'
            )
        )
)
select
    *
from
    cleaned
