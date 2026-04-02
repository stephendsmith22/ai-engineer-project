from google.cloud import bigquery

NEWS_SCHEMA = [
    bigquery.SchemaField("article_id", "STRING"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("source_name", "STRING"),
    bigquery.SchemaField("authors", "STRING", mode="REPEATED"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("url_to_image", "STRING"),
    bigquery.SchemaField("published_at", "TIMESTAMP"),
    bigquery.SchemaField(
        "created_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"
    ),
    bigquery.SchemaField("content", "STRING"),
]

WEATHER_SCHEMA = [
    bigquery.SchemaField("forecast_id", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField(
        "created_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"
    ),
    bigquery.SchemaField("datetime", "TIMESTAMP"),
    bigquery.SchemaField("temperature", "FLOAT"),
    bigquery.SchemaField("feels_like", "FLOAT"),
    bigquery.SchemaField("temp_min", "FLOAT"),
    bigquery.SchemaField("temp_max", "FLOAT"),
    bigquery.SchemaField("humidity", "INTEGER"),
    bigquery.SchemaField("weather_main", "STRING"),
    bigquery.SchemaField("weather_description", "STRING"),
    bigquery.SchemaField("wind_speed", "FLOAT"),
    bigquery.SchemaField("visibility", "INTEGER"),
]

LOGS_SCHEMA = [
    bigquery.SchemaField("log_id", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("api_source", "STRING"),
    bigquery.SchemaField("error_type", "STRING"),
    bigquery.SchemaField("error_message", "STRING"),
]

RUNS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("pipeline_run_id", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("api_source", "STRING"),
    bigquery.SchemaField("records_fetched", "INTEGER"),
    bigquery.SchemaField("records_loaded", "INTEGER"),
    bigquery.SchemaField("errors", "INTEGER"),
    bigquery.SchemaField("status", "STRING"),
]
