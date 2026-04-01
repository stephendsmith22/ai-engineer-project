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
