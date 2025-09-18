CREATE schema IF NOT EXISTS raw;

-- title.akas
DROP TABLE IF EXISTS raw.title_akas;

CREATE TABLE raw.title_akas (
    titleId text,
    ordering text,
    title text,
    region text,
    language text,
    TYPES text,
    -- comma-separated values in TSV
    attributes text,
    -- comma-separated values
    isOriginalTitle text
);

-- title.basics
DROP TABLE IF EXISTS raw.title_basics;

CREATE TABLE raw.title_basics (
    tconst text PRIMARY KEY,
    titleType text,
    primaryTitle text,
    originalTitle text,
    isAdult text,
    startYear text,
    endYear text,
    runtimeMinutes text,
    genres text -- comma-separated values
);

-- title.principals
DROP TABLE IF EXISTS raw.title_principals;

CREATE TABLE raw.title_principals (
    tconst text,
    ordering text,
    nconst text,
    category text,
    job text,
    characters text
);

-- title.ratings
DROP TABLE IF EXISTS raw.title_ratings;

CREATE TABLE raw.title_ratings (
    tconst text PRIMARY KEY,
    averageRating text,
    numVotes text
);

-- name.basics
DROP TABLE IF EXISTS raw.name_basics;

CREATE TABLE raw.name_basics (
    nconst text PRIMARY KEY,
    primaryName text,
    birthYear text,
    deathYear text,
    primaryProfession text,
    -- comma-separated
    knownForTitles text -- comma-separated tconsts
);