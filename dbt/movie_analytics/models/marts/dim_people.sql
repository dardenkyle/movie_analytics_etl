{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with people_base as (
    select * from {{ ref('stg_name_basics') }}
),

people_enhanced as (
    select
        -- Primary key
        person_id,

        -- Basic information
        primary_name,

        -- Life information
        birth_year,
        death_year,
        case
            when death_year is not null then death_year - birth_year
            when birth_year is not null then extract(year from current_date) - birth_year
            else null
        end as age_or_current_age,

        case
            when death_year is not null then true
            else false
        end as is_deceased,

        -- Career timeline
        case
            when birth_year is not null then floor(birth_year / 10) * 10
            else null
        end as birth_decade,

        case
            when death_year is not null then floor(death_year / 10) * 10
            else null
        end as death_decade,

        -- Age categories
        case
            when birth_year is null then 'Unknown'
            when extract(year from current_date) - birth_year < 30 then 'Young (Under 30)'
            when extract(year from current_date) - birth_year < 50 then 'Mid-Career (30-49)'
            when extract(year from current_date) - birth_year < 70 then 'Veteran (50-69)'
            else 'Elder (70+)'
        end as age_category,

        -- Professional information
        professions_raw,
        case
            when professions_raw is not null then
                array_length(string_to_array(professions_raw, ','), 1)
            else 0
        end as profession_count,

        -- Primary profession (first in list)
        case
            when professions_raw is not null then
                trim(split_part(professions_raw, ',', 1))
            else null
        end as primary_profession,

        -- Career flags
        case
            when professions_raw ilike '%actor%' or professions_raw ilike '%actress%'
            then true else false
        end as is_actor,

        case
            when professions_raw ilike '%director%'
            then true else false
        end as is_director,

        case
            when professions_raw ilike '%writer%'
            then true else false
        end as is_writer,

        case
            when professions_raw ilike '%producer%'
            then true else false
        end as is_producer,

        case
            when professions_raw ilike '%composer%'
            then true else false
        end as is_composer,

        -- Multi-role analysis
        case
            when professions_raw is not null and
                 array_length(string_to_array(professions_raw, ','), 1) > 1 then true
            else false
        end as is_multi_role_professional,

        -- Known for titles
        known_for_titles_raw,
        case
            when known_for_titles_raw is not null then
                array_length(string_to_array(known_for_titles_raw, ','), 1)
            else 0
        end as known_for_title_count,

        -- Name analysis
        length(primary_name) as name_character_count,
        case
            when position(' ' in primary_name) > 0 then
                trim(substring(primary_name from 1 for position(' ' in primary_name) - 1))
            else primary_name
        end as first_name,

        case
            when position(' ' in primary_name) > 0 then
                trim(substring(primary_name from position(' ' in primary_name) + 1))
            else null
        end as last_name,

        -- Career longevity estimates (birth year to current year or death year)
        case
            when birth_year is not null then
                coalesce(death_year, extract(year from current_date)) - birth_year
            else null
        end as potential_career_span_years,

        -- Generation classification
        case
            when birth_year is null then 'Unknown Generation'
            when birth_year between 1928 and 1945 then 'Silent Generation'
            when birth_year between 1946 and 1964 then 'Baby Boomers'
            when birth_year between 1965 and 1980 then 'Generation X'
            when birth_year between 1981 and 1996 then 'Millennials'
            when birth_year between 1997 and 2012 then 'Generation Z'
            when birth_year > 2012 then 'Generation Alpha'
            else 'Pre-Silent Generation'
        end as generation

    from people_base
)

select * from people_enhanced
