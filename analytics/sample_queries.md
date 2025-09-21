# Sample Analytics Queries for Movie Analytics ETL

This document demonstrates the analytical capabilities of the movie data mart with practical business questions and insights.

## Movie Trends Analysis

### 1. Top-Rated Movies by Decade

```sql
-- Find the highest-rated movies from each decade with statistical significance
SELECT
    decade,
    primary_title,
    average_rating,
    num_votes,
    success_category,
    runtime_category
FROM fact_ratings fr
JOIN dim_titles dt ON fr.title_id = dt.title_id
WHERE
    fr.is_statistically_significant = true
    AND dt.content_category = 'Movie'
    AND decade IS NOT NULL
    AND average_rating >= 8.0
ORDER BY decade DESC, average_rating DESC
LIMIT 20;
```

### 2. Movie Production Trends Over Time

```sql
-- Analyze movie production volume and average ratings by decade
SELECT
    decade,
    COUNT(*) as movie_count,
    AVG(CASE WHEN average_rating IS NOT NULL THEN average_rating END) as avg_rating,
    COUNT(CASE WHEN is_highly_rated_popular THEN 1 END) as critically_acclaimed_count,
    AVG(runtime_minutes) as avg_runtime
FROM dim_titles dt
LEFT JOIN fact_ratings fr ON dt.title_id = fr.title_id
WHERE
    dt.content_category = 'Movie'
    AND decade IS NOT NULL
    AND decade >= 1920
GROUP BY decade
ORDER BY decade;
```

### 3. Genre Popularity Analysis

```sql
-- Most popular genres by vote volume (requires genre parsing)
WITH genre_stats AS (
    SELECT
        TRIM(unnest(string_to_array(genres_raw, ','))) as genre,
        COUNT(*) as title_count,
        AVG(fr.average_rating) as avg_rating,
        SUM(fr.num_votes) as total_votes
    FROM dim_titles dt
    JOIN fact_ratings fr ON dt.title_id = fr.title_id
    WHERE
        dt.genres_raw IS NOT NULL
        AND fr.num_votes IS NOT NULL
        AND dt.content_category = 'Movie'
    GROUP BY TRIM(unnest(string_to_array(genres_raw, ',')))
)
SELECT
    genre,
    title_count,
    ROUND(avg_rating, 2) as avg_rating,
    total_votes,
    ROUND(total_votes::numeric / title_count, 0) as avg_votes_per_movie
FROM genre_stats
WHERE title_count >= 100  -- Filter for genres with significant representation
ORDER BY total_votes DESC
LIMIT 15;
```

## People & Career Analysis

### 4. Most Prolific Professions by Generation

```sql
-- Analyze career patterns across generations
SELECT
    generation,
    primary_profession,
    COUNT(*) as person_count,
    AVG(age_or_current_age) as avg_age,
    COUNT(CASE WHEN is_multi_role_professional THEN 1 END) as multi_role_count
FROM dim_people
WHERE
    primary_profession IS NOT NULL
    AND generation != 'Unknown Generation'
GROUP BY generation, primary_profession
HAVING COUNT(*) >= 50
ORDER BY generation, person_count DESC;
```

### 5. Career Longevity Analysis

```sql
-- People with longest potential career spans
SELECT
    primary_name,
    primary_profession,
    birth_year,
    death_year,
    potential_career_span_years,
    known_for_title_count,
    generation
FROM dim_people
WHERE
    potential_career_span_years IS NOT NULL
    AND potential_career_span_years > 60  -- 60+ year careers
    AND known_for_title_count > 0
ORDER BY potential_career_span_years DESC
LIMIT 20;
```

## Content & Rating Analysis

### 6. Quality vs Popularity Quadrant Analysis

```sql
-- Analyze the distribution of movies across quality/popularity quadrants
SELECT
    quality_popularity_quadrant,
    COUNT(*) as movie_count,
    ROUND(AVG(average_rating), 2) as avg_rating,
    ROUND(AVG(num_votes), 0) as avg_votes,
    COUNT(CASE WHEN decade >= 2010 THEN 1 END) as recent_movies
FROM fact_ratings fr
JOIN dim_titles dt ON fr.title_id = dt.title_id
WHERE dt.content_category = 'Movie'
GROUP BY quality_popularity_quadrant
ORDER BY
    CASE quality_popularity_quadrant
        WHEN 'High Quality, High Popularity' THEN 1
        WHEN 'High Quality, Low Popularity' THEN 2
        WHEN 'Low Quality, High Popularity' THEN 3
        WHEN 'Low Quality, Low Popularity' THEN 4
    END;
```

### 7. Runtime Evolution Analysis

```sql
-- How movie runtimes have changed over time
SELECT
    decade,
    runtime_category,
    COUNT(*) as movie_count,
    ROUND(AVG(runtime_minutes), 1) as avg_runtime,
    ROUND(AVG(average_rating), 2) as avg_rating
FROM dim_titles dt
LEFT JOIN fact_ratings fr ON dt.title_id = fr.title_id
WHERE
    dt.content_category = 'Movie'
    AND decade IS NOT NULL
    AND runtime_minutes IS NOT NULL
    AND decade >= 1950
GROUP BY decade, runtime_category
ORDER BY decade, runtime_category;
```

### 8. Adult Content vs General Audience Analysis

```sql
-- Compare adult content vs general audience content performance
SELECT
    content_rating_category,
    COUNT(*) as title_count,
    ROUND(AVG(average_rating), 2) as avg_rating,
    ROUND(AVG(num_votes), 0) as avg_votes,
    COUNT(CASE WHEN is_statistically_significant THEN 1 END) as significant_ratings_count,
    COUNT(CASE WHEN success_category = 'Critical Acclaim' THEN 1 END) as critically_acclaimed
FROM dim_titles dt
JOIN fact_ratings fr ON dt.title_id = fr.title_id
WHERE dt.content_category = 'Movie'
GROUP BY content_rating_category;
```

## TV Series Analysis

### 9. Long-Running TV Series Analysis

```sql
-- Analyze long-running TV series
SELECT
    primary_title,
    start_year,
    end_year,
    series_duration_years,
    average_rating,
    num_votes,
    is_ongoing_series
FROM dim_titles dt
LEFT JOIN fact_ratings fr ON dt.title_id = fr.title_id
WHERE
    dt.content_category = 'TV Series'
    AND series_duration_years IS NOT NULL
    AND series_duration_years >= 10  -- 10+ year series
ORDER BY series_duration_years DESC, average_rating DESC
LIMIT 20;
```

### 10. Recent vs Classic Content Performance

```sql
-- Compare recent titles (last 5 years) vs classic content
SELECT
    CASE WHEN is_recent_title THEN 'Recent (2020+)' ELSE 'Classic (Pre-2020)' END as era,
    content_category,
    COUNT(*) as title_count,
    ROUND(AVG(average_rating), 2) as avg_rating,
    COUNT(CASE WHEN is_highly_rated_popular THEN 1 END) as highly_rated_popular,
    COUNT(CASE WHEN success_category = 'Critical Acclaim' THEN 1 END) as critical_acclaim
FROM dim_titles dt
JOIN fact_ratings fr ON dt.title_id = fr.title_id
WHERE average_rating IS NOT NULL
GROUP BY is_recent_title, content_category
ORDER BY era DESC, content_category;
```

## Data Quality Insights

### 11. Data Completeness Analysis

```sql
-- Analyze data completeness across the mart
SELECT
    'dim_titles' as table_name,
    COUNT(*) as total_records,
    COUNT(primary_title) as has_title,
    COUNT(start_year) as has_year,
    COUNT(runtime_minutes) as has_runtime,
    COUNT(genres_raw) as has_genres,
    ROUND(100.0 * COUNT(start_year) / COUNT(*), 1) as year_completeness_pct
FROM dim_titles

UNION ALL

SELECT
    'dim_people' as table_name,
    COUNT(*) as total_records,
    COUNT(primary_name) as has_name,
    COUNT(birth_year) as has_birth_year,
    COUNT(professions_raw) as has_professions,
    COUNT(known_for_titles_raw) as has_known_for,
    ROUND(100.0 * COUNT(birth_year) / COUNT(*), 1) as birth_year_completeness_pct
FROM dim_people

UNION ALL

SELECT
    'fact_ratings' as table_name,
    COUNT(*) as total_records,
    COUNT(average_rating) as has_rating,
    COUNT(num_votes) as has_votes,
    COUNT(title_id) as has_title_link,
    COUNT(CASE WHEN is_statistically_significant THEN 1 END) as statistically_significant,
    ROUND(100.0 * COUNT(CASE WHEN is_statistically_significant THEN 1 END) / COUNT(*), 1) as significant_pct
FROM fact_ratings;
```

## Business Intelligence Summary

### 12. Executive Dashboard Query

```sql
-- High-level metrics for business reporting
SELECT
    'Content Library' as metric_category,
    'Total Movies' as metric_name,
    COUNT(*)::text as metric_value
FROM dim_titles
WHERE content_category = 'Movie'

UNION ALL

SELECT 'Content Library', 'Total TV Series', COUNT(*)::text
FROM dim_titles
WHERE content_category = 'TV Series'

UNION ALL

SELECT 'Quality Metrics', 'Highly Rated & Popular', COUNT(*)::text
FROM fact_ratings
WHERE is_highly_rated_popular = true

UNION ALL

SELECT 'Quality Metrics', 'Critical Acclaim', COUNT(*)::text
FROM fact_ratings
WHERE success_category = 'Critical Acclaim'

UNION ALL

SELECT 'People Database', 'Total People', COUNT(*)::text
FROM dim_people

UNION ALL

SELECT 'People Database', 'Active Directors', COUNT(*)::text
FROM dim_people
WHERE is_director = true

UNION ALL

SELECT 'Data Completeness', 'Statistically Significant Ratings', COUNT(*)::text
FROM fact_ratings
WHERE is_statistically_significant = true;
```

---

## How to Use These Queries

1. **Connect to your database**:

   ```bash
   docker compose exec postgres psql -U postgres -d analytics
   ```

2. **Run queries in the marts schema**:

   ```sql
   SET search_path TO staging_marts, public;
   ```

3. **Export results for visualization**:
   ```sql
   \copy (YOUR_QUERY_HERE) TO '/tmp/results.csv' WITH CSV HEADER;
   ```

These queries demonstrate the business value and analytical power of your movie data mart!
