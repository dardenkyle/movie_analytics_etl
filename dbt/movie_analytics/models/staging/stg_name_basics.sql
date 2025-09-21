{{ config(
    materialized = 'view',
    schema = 'staging'
) }} with source as (
    select
        *
    from
        {{ source('raw', 'name_basics') }}
),
cleaned as (
    select
        -- Primary key
        nconst as person_id,
        -- Basic person information
        primaryname as primary_name,
        -- Years - convert \N to null and cast to integer
        case
            when birthyear = '\\N' then null
            else birthyear :: integer
        end as birth_year,
        case
            when deathyear = '\\N' then null
            else deathyear :: integer
        end as death_year,
        -- Comma-separated fields - keep as text for now, will split in marts
        case
            when primaryprofession = '\\N' then null
            else primaryprofession
        end as professions_raw,
        case
            when knownfortitles = '\\N' then null
            else knownfortitles
        end as known_for_titles_raw
    from
        source
    -- Filter out records with missing primary names
    where
        primaryname is not null
        and primaryname != '\\N'
        and trim(primaryname) != ''
)
select
    *
from
    cleaned
