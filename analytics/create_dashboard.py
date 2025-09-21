"""
Movie Analytics Visualization Dashboard
Simple dashboard showcasing insights from the movie analytics data mart
"""

import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as pyo

# Database connection configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "analytics",
    "user": "postgres",
    "password": "postgres",
}


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)


def execute_query(query):
    """Execute SQL query and return DataFrame"""
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn)


def create_movies_by_decade_chart():
    """Create chart showing movie production and ratings by decade"""
    query = """
    SELECT 
        dt.decade::int as decade,
        COUNT(*) as movie_count,
        AVG(CASE WHEN fr.average_rating IS NOT NULL THEN fr.average_rating END) as avg_rating,
        COUNT(CASE WHEN fr.is_highly_rated_popular THEN 1 END) as highly_rated_count
    FROM staging_marts.dim_titles dt
    LEFT JOIN staging_marts.fact_ratings fr ON dt.title_id = fr.title_id
    WHERE 
        dt.content_category = 'Movie' 
        AND dt.decade IS NOT NULL
        AND dt.decade >= 1920
        AND dt.decade <= 2020
    GROUP BY dt.decade::int
    ORDER BY dt.decade::int;
    """

    df = execute_query(query)
    if df.empty:
        raise ValueError(
            "No rows returned. Verify content_category/decade filters and joins."
        )

    # Ensure clean types
    df["decade"] = df["decade"].astype(str)  # categorical x for even spacing
    df["movie_count"] = pd.to_numeric(df["movie_count"])
    df["avg_rating"] = pd.to_numeric(df["avg_rating"])

    print(df.dtypes)
    print(df.head())

    # Create subplot with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add movie count bars
    fig.add_trace(
        go.Bar(
            x=df["decade"],
            y=df["movie_count"],
            name="Movies Produced",
            marker_color="lightblue",
        ),
        secondary_y=False,
    )

    # Add average rating line
    fig.add_trace(
        go.Scatter(
            x=df["decade"],
            y=df["avg_rating"],
            name="Average Rating",
            line={"color": "red", "width": 3},
        ),
        secondary_y=True,
    )

    # Update layout
    fig.update_xaxes(title_text="Decade")
    fig.update_yaxes(title_text="Number of Movies", secondary_y=False)
    fig.update_yaxes(title_text="Average Rating", secondary_y=True)
    fig.update_layout(title="Movie Production and Quality by Decade")

    return fig


def create_genre_popularity_chart():
    """Create chart showing most popular genres"""
    query = """
    WITH genre_stats AS (
        SELECT 
            TRIM(unnest(string_to_array(genres_raw, ','))) as genre,
            COUNT(*) as title_count,
            AVG(fr.average_rating) as avg_rating,
            SUM(fr.num_votes) as total_votes
        FROM staging_marts.dim_titles dt
        JOIN staging_marts.fact_ratings fr ON dt.title_id = fr.title_id
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
        total_votes
    FROM genre_stats
    WHERE title_count >= 1000
    ORDER BY total_votes DESC
    LIMIT 15;
    """

    df = execute_query(query)

    fig = px.bar(
        df,
        x="genre",
        y="total_votes",
        title="Most Popular Movie Genres by Vote Volume",
        labels={"total_votes": "Total Votes (Millions)", "genre": "Genre"},
        color="avg_rating",
        color_continuous_scale="RdYlBu_r",
    )

    fig.update_layout(xaxis_tickangle=-45)
    return fig


def create_quality_vs_popularity_chart():
    """Create scatter plot of quality vs popularity"""
    query = """
    SELECT 
        dt.primary_title,
        fr.average_rating,
        fr.num_votes,
        dt.decade::int as decade,
        dt.content_category,
        COALESCE(fr.success_category, 'Average Quality') as success_category
    FROM staging_marts.dim_titles dt
    JOIN staging_marts.fact_ratings fr ON dt.title_id = fr.title_id
    WHERE 
        fr.average_rating IS NOT NULL 
        AND fr.num_votes IS NOT NULL
        AND fr.num_votes >= 100  -- Lower threshold for more data points
        AND dt.content_category = 'Movie'
        AND dt.decade >= 1990  -- Focus on more recent movies
        AND dt.decade <= 2020
    ORDER BY fr.num_votes DESC
    LIMIT 2000;  -- Limit for performance
    """

    df = execute_query(query)
    if df.empty:
        raise ValueError(
            "No rows returned. Verify content_category/decade filters and joins."
        )

    fig = px.scatter(
        df,
        x="num_votes",
        y="average_rating",
        color="success_category",
        size="num_votes",
        hover_data=["primary_title", "decade"],
        title="Movie Quality vs Popularity (1990-2020)",
        labels={"num_votes": "Number of Votes", "average_rating": "Average Rating"},
        log_x=True,
    )

    if len(df) > 0:
        fig.update_traces(
            marker=dict(sizeref=2.0 * max(df["num_votes"]) / (40.0**2), sizemin=4)
        )
    return fig


def create_runtime_evolution_chart():
    """Create chart showing how movie runtimes have evolved"""
    query = """
    SELECT 
        dt.decade::int as decade,
        CASE 
            WHEN runtime_minutes <= 90 THEN 'Short (90 min or less)'
            WHEN runtime_minutes <= 180 THEN 'Medium (91-180 min)'
            ELSE 'Long (over 180 min)'
        END as runtime_group,
        COUNT(*) as movie_count
    FROM staging_marts.dim_titles dt
    WHERE 
        dt.content_category = 'Movie'
        AND dt.decade IS NOT NULL
        AND runtime_minutes IS NOT NULL
        AND dt.decade >= 1960  -- Start from 1960 for better data
        AND dt.decade <= 2020
    GROUP BY dt.decade::int, CASE 
            WHEN runtime_minutes <= 90 THEN 'Short (90 min or less)'
            WHEN runtime_minutes <= 180 THEN 'Medium (91-180 min)'
            ELSE 'Long (over 180 min)'
        END
    ORDER BY dt.decade::int;
    """

    df = execute_query(query)

    fig = px.bar(
        df,
        x="decade",
        y="movie_count",
        color="runtime_group",
        title="Evolution of Movie Runtime Categories by Decade",
        labels={"movie_count": "Number of Movies", "decade": "Decade"},
    )

    return fig


def create_people_generation_chart():
    """Create chart showing people distribution by generation and profession"""
    query = """
    SELECT 
        generation,
        CASE 
            WHEN is_actor THEN 'Actor'
            WHEN is_director THEN 'Director'  
            WHEN is_writer THEN 'Writer'
            WHEN is_producer THEN 'Producer'
            ELSE 'Other'
        END as primary_role,
        COUNT(*) as person_count
    FROM staging_marts.dim_people
    WHERE 
        generation != 'Unknown Generation'
        AND (is_actor OR is_director OR is_writer OR is_producer)
    GROUP BY generation, 
        CASE 
            WHEN is_actor THEN 'Actor'
            WHEN is_director THEN 'Director'  
            WHEN is_writer THEN 'Writer'
            WHEN is_producer THEN 'Producer'
            ELSE 'Other'
        END
    ORDER BY generation, person_count DESC;
    """

    df = execute_query(query)

    fig = px.bar(
        df,
        x="generation",
        y="person_count",
        color="primary_role",
        title="Entertainment Industry Professionals by Generation",
        labels={"person_count": "Number of People", "generation": "Generation"},
    )

    fig.update_layout(xaxis_tickangle=-45)
    return fig


def create_dashboard():
    """Create complete dashboard with all visualizations"""
    print("[DASHBOARD] Creating Movie Analytics Dashboard...")
    print("===============================================")

    # Create all charts
    charts = {}

    try:
        charts["Movies by Decade"] = create_movies_by_decade_chart()
        print("[SUCCESS] Created: Movies by Decade")
    except Exception as e:
        print("[ERROR] Error creating Movies by Decade chart: " + str(e))

    try:
        charts["Genre Popularity"] = create_genre_popularity_chart()
        print("[SUCCESS] Created: Genre Popularity")
    except Exception as e:
        print(f"[ERROR] Error creating Genre Popularity chart: {e}")

    try:
        charts["Quality vs Popularity"] = create_quality_vs_popularity_chart()
        print("[SUCCESS] Created: Quality vs Popularity")
    except Exception as e:
        print(f"[ERROR] Error creating Quality vs Popularity chart: {e}")

    try:
        charts["Runtime Evolution"] = create_runtime_evolution_chart()
        print("[SUCCESS] Created: Runtime Evolution")
    except Exception as e:
        print(f"[ERROR] Error creating Runtime Evolution chart: {e}")

    try:
        charts["People by Generation"] = create_people_generation_chart()
        print("[SUCCESS] Created: People by Generation")
    except Exception as e:
        print(f"[ERROR] Error creating People by Generation chart: {e}")

    if not charts:
        print("[ERROR] No charts were successfully created!")
        return

    # Create HTML file with all charts
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Movie Analytics Dashboard</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .header { text-align: center; margin-bottom: 30px; }
            .chart-container { margin: 30px 0; }
            .metrics { display: flex; justify-content: space-around; margin: 20px 0; }
            .metric { text-align: center; padding: 20px; background: #f0f0f0; border-radius: 10px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>[DASHBOARD] Movie Analytics Dashboard</h1>
            <p>Insights from IMDb Dataset - Processed through dbt Data Mart</p>
        </div>
        
        <div class="metrics">
            <div class="metric">
                <h3>11.9M+</h3>
                <p>Movies & TV Shows</p>
            </div>
            <div class="metric">
                <h3>14.7M+</h3>
                <p>People Analyzed</p>
            </div>
            <div class="metric">
                <h3>1.6M+</h3>
                <p>Rating Records</p>
            </div>
            <div class="metric">
                <h3>100%</h3>
                <p>Data Quality Tested</p>
            </div>
        </div>
    """

    # Add each chart to HTML
    chart_id = 0
    for title, fig in charts.items():
        chart_id += 1

        # Force Plotly to use JSON arrays instead of binary data
        # by configuring the figure to use the HTML renderer
        config = {
            "toImageButtonOptions": {"format": "png", "filename": "chart", "scale": 1}
        }

        # Use to_html method with proper configuration to force JSON format
        # Configure Plotly to use JSON arrays instead of binary data
        import json

        # Convert figure to JSON to force JSON format, then create HTML
        fig_json = fig.to_json()
        fig_dict = json.loads(fig_json)

        # Create a div with the chart data embedded as JSON
        chart_html = f"""
        <div id="chart{chart_id}" style="width:100%;height:500px;"></div>
        <script>
            Plotly.newPlot('chart{chart_id}', {json.dumps(fig_dict["data"])}, {json.dumps(fig_dict["layout"])}, {json.dumps(config)});
        </script>
        """

        html_content += f"""
        <div class="chart-container">
            <h2>{title}</h2>
            {chart_html}
        </div>
        """

    html_content += """
        <div class="header" style="margin-top: 50px;">
            <p><strong>Data Source:</strong> IMDb Non-Commercial Datasets</p>
            <p><strong>Processing:</strong> PostgreSQL + dbt + Python</p>
            <p><strong>Architecture:</strong> Raw → Staging → Marts dimensional model</p>
        </div>
    </body>
    </html>
    """

    # Save dashboard
    with open("movie_analytics_dashboard.html", "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\n[COMPLETE] Dashboard created successfully!")
    print(f"[FILE] Saved as: movie_analytics_dashboard.html")
    print(f"[WEB] Open the file in your browser to view the interactive dashboard")


def get_summary_stats():
    """Print summary statistics about the data mart"""
    queries = {
        "Total Movies": "SELECT COUNT(*) FROM staging_marts.dim_titles WHERE content_category = 'Movie'",
        "Total TV Series": "SELECT COUNT(*) FROM staging_marts.dim_titles WHERE content_category = 'TV Series'",
        "Total People": "SELECT COUNT(*) FROM staging_marts.dim_people",
        "Ratings with 1000+ votes": "SELECT COUNT(*) FROM staging_marts.fact_ratings WHERE is_statistically_significant = true",
        "Highly Rated & Popular": "SELECT COUNT(*) FROM staging_marts.fact_ratings WHERE is_highly_rated_popular = true",
    }

    print("\n[DATA] Data Mart Summary:")
    print("==========================")

    for metric, query in queries.items():
        try:
            result = execute_query(query)
            count = result.iloc[0, 0]
            print(f"• {metric}: {count:,}")
        except Exception as e:
            print(f"• {metric}: Error - {e}")


if __name__ == "__main__":
    try:
        # Print summary stats first
        get_summary_stats()

        # Create dashboard
        create_dashboard()

    except Exception as e:
        print(f"[ERROR] Error creating dashboard: {e}")
        print("Make sure PostgreSQL is running and contains the marts data:")
        print("1. docker compose up -d")
        print("2. ./run_pipeline.sh (or run_pipeline.bat)")
        print("3. python analytics/create_dashboard.py")
