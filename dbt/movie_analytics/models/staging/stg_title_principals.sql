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
)
select
    *
from
    cleaned