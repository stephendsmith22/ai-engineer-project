-- News Pipeline: news_pipeline.top_headlines
CREATE TABLE IF NOT EXISTS news_pipeline.top_headlines (
    article_id      STRING,   -- SHA256 hash of article URL (primary key)
    source_id       STRING,
    source_name     STRING,
    authors         ARRAY<STRING>,
    title           STRING,
    description     STRING,
    url             STRING,
    url_to_image    STRING,
    published_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    content         STRING
);

-- Weather Pipeline: weather_pipeline.weather_forecast
CREATE TABLE IF NOT EXISTS weather_pipeline.weather_forecast (
    forecast_id         STRING,   -- SHA256 hash of city + datetime (primary key)
    city                STRING,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    datetime            TIMESTAMP,
    temperature         FLOAT64,
    feels_like          FLOAT64,
    temp_min            FLOAT64,
    temp_max            FLOAT64,
    humidity            INT64,
    weather_main        STRING,
    weather_description STRING,
    wind_speed          FLOAT64,
    visibility          INT64
);

-- Error Tracking: pipeline_logs.api_errors
CREATE TABLE IF NOT EXISTS pipeline_logs.api_errors (
    log_id        STRING,     -- SHA256 hash of source + type + message + timestamp (primary key)
    timestamp     TIMESTAMP,  -- When the error occurred
    api_source    STRING,
    error_type    STRING,     -- "Timeout", "HTTPError", "Exception"
    error_message STRING      -- Full error message
);

-- Activity Log: pipeline_logs.pipeline_runs
CREATE TABLE IF NOT EXISTS pipeline_logs.pipeline_runs (
    run_id          STRING,   -- SHA256 hash of api_source + timestamp (primary key)
    pipeline_run_id STRING,   -- Shared ID across all pipelines in the same execution
    timestamp       TIMESTAMP,
    api_source      STRING,
    records_fetched INTEGER,  -- Records returned by API
    records_loaded  INTEGER,  -- Records actually inserted into BigQuery
    errors          INTEGER,  -- Validation failures
    status          STRING    -- "SUCCESS", "PARTIAL", or "FAILED"
);
