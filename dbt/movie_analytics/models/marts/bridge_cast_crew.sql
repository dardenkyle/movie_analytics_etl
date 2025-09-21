{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with principals_base as (
    select * from {{ ref('stg_title_principals') }}
),

title_context as (
    select
        title_id,
        title_type,
        content_category,
        start_year,
        decade,
        is_adult
    from {{ ref('dim_titles') }}
),

person_context as (
    select
        person_id,
        primary_name,
        age_category,
        is_actor,
        is_director,
        is_writer,
        is_producer,
        generation
    from {{ ref('dim_people') }}
),

bridge_enhanced as (
    select
        -- Composite key components
        p.title_id,
        p.person_id,
        p.ordering,

        -- Basic role information
        p.job_category,
        p.job_title,
        p.character_names,

        -- Title context
        t.title_type,
        t.content_category,
        t.start_year,
        t.decade,
        t.is_adult,

        -- Person context
        pe.primary_name,
        pe.age_category,
        pe.generation,

        -- Role categorization
        case
            when p.job_category in ('actor', 'actress') then 'Acting'
            when p.job_category in ('director') then 'Directing'
            when p.job_category in ('writer') then 'Writing'
            when p.job_category in ('producer', 'executive producer') then 'Producing'
            when p.job_category in ('composer', 'music_department') then 'Music'
            when p.job_category in ('cinematographer', 'camera_department') then 'Cinematography'
            when p.job_category in ('editor') then 'Editing'
            else 'Other'
        end as role_department,

        -- Credit importance (based on ordering)
        case
            when p.ordering is null then 'Unspecified'
            when p.ordering <= 3 then 'Lead/Principal'
            when p.ordering <= 10 then 'Supporting'
            when p.ordering <= 50 then 'Featured'
            else 'Background'
        end as credit_importance,

        -- Character analysis (for actors)
        case
            when p.job_category in ('actor', 'actress') and p.character_names is not null then
                array_length(string_to_array(p.character_names, ','), 1)
            else 0
        end as character_count,

        -- Content type alignment
        case
            when t.content_category = 'Movie' then 'Film'
            when t.content_category = 'TV Series' then 'Television'
            when t.content_category = 'TV Episode' then 'Episode'
            else t.content_category
        end as media_type,

        -- Era context
        case
            when t.start_year is null then 'Unknown Era'
            when t.start_year < 1950 then 'Golden Age (Pre-1950)'
            when t.start_year < 1970 then 'Classic Era (1950-1969)'
            when t.start_year < 1990 then 'Modern Era (1970-1989)'
            when t.start_year < 2010 then 'Digital Era (1990-2009)'
            else 'Streaming Era (2010+)'
        end as industry_era

    from principals_base p
    left join title_context t on p.title_id = t.title_id
    left join person_context pe on p.person_id = pe.person_id
)

select * from bridge_enhanced
