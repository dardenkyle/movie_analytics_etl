{{ config(
    materialized = 'view',
    schema = 'staging'
) }} with source as (
    select
        *
    from
        {{ source('raw', 'title_principals') }}
),
cleaned as (
    select
        -- Foreign keys
        tconst as title_id,
        nconst as person_id,
        -- Ordering and role information
        case
            when ordering = '\\N' then null
            else ordering :: integer
        end as ordering,
        category as job_category,
        case
            when job = '\\N' then null
            else job
        end as job_title,
        case
            when characters = '\\N' then null
            else characters
        end as character_names
    from
        source
    -- Only include records for titles and names that exist in their respective tables
    where
        tconst in (
            select tconst
            from {{ source('raw', 'title_basics') }}
            where titletype in (
                'movie', 'short', 'tvEpisode', 'tvMiniSeries', 'tvMovie',
                'tvSeries', 'tvShort', 'tvSpecial', 'video', 'videoGame'
            )
        )
        and nconst in (
            select nconst
            from {{ source('raw', 'name_basics') }}
            where primaryname is not null
            and primaryname != '\\N'
            and trim(primaryname) != ''
        )
)
select
    *
from
    cleaned
