{{ config(materialized = 'view', schema = 'staging') }} WITH source AS (
    SELECT
        *
    FROM
        {{ source('raw', 'title_ratings') }}
),
cleaned AS (
    SELECT
        tconst AS title_id,
        CASE
            WHEN averagerating = '\\N' THEN NULL
            ELSE averagerating :: decimal(3, 1)
        END AS average_rating,
        CASE
            WHEN numvotes = '\\N' THEN NULL
            ELSE numvotes :: integer
        END AS num_votes
    FROM
        source
)
SELECT
    *
FROM
    cleaned
