# AI Engineer Project

## Overview

An automated data pipeline that fetches US top news headlines and 5-day weather forecast data from public APIs, validates and deduplicates the data, and loads it into Google BigQuery for analysis. The pipeline runs on a daily schedule via GitHub Actions and sends real-time alerts to Discord on success or failure. All services used are within their free tiers.

---

## Architecture

```
NewsAPI                    OpenWeatherMap API
    |                              |
    └──────────┬───────────────────┘
               ↓
           main.py
      ┌────────────────┐
      │  Fetch from API│
      │  Validate data │
      │  Deduplicate   │
      │  Load to BQ    │
      └────────────────┘
               ↓
        Google BigQuery
    ┌──────────────────────┐
    │ news_pipeline        │
    │ weather_pipeline     │
    │ pipeline_logs        │
    └──────────────────────┘
               ↓
     Discord (alerts on every run)
```

---

## Data Sources

### NewsAPI — Top Headlines
**Why:** Provides structured, real-time US news headlines with metadata (author, source, published date) that maps cleanly to a relational schema. The free tier is sufficient for a daily batch pipeline.

- **Endpoint:** `GET /v2/top-headlines?country=us`
- **Free tier limit:** 100 requests/day, articles up to 30 days old
- **Records per call:** Up to 100 articles

### OpenWeatherMap — 5-Day Forecast
**Why:** Offers a reliable 5-day / 3-hour forecast API with no credit card required on the free tier. Provides rich weather data (temperature, humidity, wind, visibility) at regular intervals suitable for time-series storage.

- **Endpoint:** `GET /data/2.5/forecast`
- **Free tier limit:** 1,000 requests/day
- **Records per call:** Up to 40 forecast slots (5 days × 8 per day)

---

## Schema Design

Primary keys are SHA-256 hashes rather than auto-increment integers because BigQuery is a data warehouse, not a relational database — it has no native auto-increment. Hashing a natural key (URL for articles, city+datetime for forecasts) produces a stable, reproducible ID that enables idempotent deduplication across runs.

See [`schema.sql`](./schema.sql) for full table definitions.

**`news_pipeline.top_headlines`**
| Column | Type | Description |
|---|---|---|
| `article_id` | STRING | SHA-256 hash of article URL (primary key) |
| `source_id` | STRING | News source identifier |
| `source_name` | STRING | News source display name |
| `authors` | ARRAY\<STRING\> | Article authors (repeated field to support co-authors) |
| `title` | STRING | Article headline |
| `description` | STRING | Article summary |
| `url` | STRING | Article URL |
| `url_to_image` | STRING | Article image URL |
| `published_at` | TIMESTAMP | Publication timestamp |
| `created_at` | TIMESTAMP | Pipeline ingestion timestamp |
| `content` | STRING | Article content snippet |

**`weather_pipeline.weather_forecast`**
| Column | Type | Description |
|---|---|---|
| `forecast_id` | STRING | SHA-256 hash of city + datetime (primary key) |
| `city` | STRING | City name |
| `datetime` | TIMESTAMP | Forecast timestamp |
| `temperature` | FLOAT64 | Temperature (°F) |
| `feels_like` | FLOAT64 | Feels like temperature (°F) |
| `temp_min` | FLOAT64 | Minimum temperature (°F) |
| `temp_max` | FLOAT64 | Maximum temperature (°F) |
| `humidity` | INT64 | Humidity percentage |
| `weather_main` | STRING | Weather condition (e.g. Rain, Clear) |
| `weather_description` | STRING | Detailed weather description |
| `wind_speed` | FLOAT64 | Wind speed (mph) |
| `visibility` | INT64 | Visibility (meters) |
| `created_at` | TIMESTAMP | Pipeline ingestion timestamp |

**`pipeline_logs.api_errors`**
| Column | Type | Description |
|---|---|---|
| `log_id` | STRING | SHA-256 hash of source + type + message + timestamp (primary key) |
| `timestamp` | TIMESTAMP | When the error occurred |
| `api_source` | STRING | `NewsAPI` or `WeatherAPI` |
| `error_type` | STRING | `Timeout`, `HTTPError`, or `Exception` |
| `error_message` | STRING | Full error message |

**`pipeline_logs.pipeline_runs`**
| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | SHA-256 hash of api_source + timestamp (primary key) |
| `pipeline_run_id` | STRING | Shared ID linking all pipelines from the same execution |
| `timestamp` | TIMESTAMP | When the run occurred |
| `api_source` | STRING | `NewsAPI` or `WeatherAPI` |
| `records_fetched` | INTEGER | Records returned by the API |
| `records_loaded` | INTEGER | Records actually inserted into BigQuery |
| `errors` | INTEGER | Records skipped due to validation failures |
| `status` | STRING | `SUCCESS`, `PARTIAL`, or `FAILED` |

---

## Setup Instructions

### Prerequisites

- Python 3.13+
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (`gcloud`)
- A Google Cloud project with BigQuery enabled
- A [NewsAPI](https://newsapi.org) account
- An [OpenWeatherMap](https://openweathermap.org) account
- A Discord server with a webhook configured

### 1. Clone the repository

```bash
git clone https://github.com/stephendsmith22/ai-engineer-project.git
cd ai-engineer-project
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the project root:

```
news_api_key=your_newsapi_key
open_weather_api_key=your_openweathermap_key
gcp_project_id=your_gcp_project_id
discord_webhook_url=your_discord_webhook_url
```

### 4. Authenticate with Google Cloud

```bash
gcloud auth application-default login
```

### 5. Run the pipeline

```bash
python main.py
```

BigQuery datasets and tables are created automatically on first run.

---

## Error Handling Strategy

| Scenario | Handling |
|---|---|
| API timeout | Caught via `requests.exceptions.Timeout`, logged to `api_errors`, Discord alert sent |
| HTTP error (4xx/5xx) | Caught via `requests.exceptions.HTTPError`, status code logged and alerted |
| Unhandled exception | Caught via broad `Exception`, full message logged and alerted |
| API schema change | Top-level key check (`articles`, `list`) before processing — alerts Discord if missing |
| Missing required fields | `validate_news_record` / `validate_weather_record` skip and log invalid records before insert |
| Invalid numeric values | Weather validation checks all numeric fields can be cast to `float` before insert |
| Duplicate records | SHA-256 primary keys queried against BigQuery before insert — duplicates filtered out |
| Rate limits | Pipeline runs once daily, well within free tier limits for both APIs |

Every run writes a row to `pipeline_logs.pipeline_runs` with status `SUCCESS`, `PARTIAL` (validation failures), or `FAILED` (exception). All errors are also written to `pipeline_logs.api_errors`.

---

## Automation

The pipeline runs daily at **9AM Eastern (1PM UTC)** via GitHub Actions. To change the schedule, update the cron value in [`.github/workflows/pipeline.yml`](./.github/workflows/pipeline.yml).

To trigger manually: **Actions tab** → **Data Pipeline** → **Run workflow**.

### GitHub Secrets Required

| Secret | Description |
|---|---|
| `NEWS_API_KEY` | NewsAPI key |
| `OPEN_WEATHER_API_KEY` | OpenWeatherMap API key |
| `DISCORD_WEBHOOK_URL` | Discord webhook URL for alerts |

GCP authentication uses Workload Identity Federation — no service account key required.

---

## Free Tier Cost Estimate

| Service | Usage per day | Free limit | % of limit used |
|---|---|---|---|
| NewsAPI | 1 request | 100/day | 1% |
| OpenWeatherMap | 1 request | 1,000/day | <1% |
| BigQuery storage | ~50 KB/day | 10 GB | <1% |
| BigQuery queries | 2 queries/run (dedup checks) | 1 TB/month | <1% |
| GitHub Actions | ~2 min/run | 2,000 min/month | ~3% |

All usage is well within free tier limits. At the current rate, BigQuery storage would take over 50 years to reach the 10 GB limit.

---

## Known Limitations

- **NewsAPI free tier** — articles older than 30 days are not accessible. Historical backfill is not possible on the free plan.
- **Top headlines only** — the pipeline fetches current US top headlines, not topic-specific or historical articles.
- **Single city weather** — weather pipeline is currently hardcoded to Miami. Multiple cities would require additional API calls.
- **Author parsing** — authors are split on commas from a single string field, which can produce incorrect splits if an author name contains a comma.
- **Content truncation** — NewsAPI truncates article content to ~200 characters on the free tier. Full article text requires a paid plan.
- **No backfill** — if the pipeline fails on a given day, that day's data is not recovered on the next run.
- **OpenWeatherMap activation delay** — newly created API keys can take up to 2 hours to activate.
