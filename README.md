# AI Engineer Project

An automated data pipeline that fetches top news headlines and weather forecast data from public APIs, validates the data, and loads it into Google BigQuery. Runs daily via GitHub Actions.

---

## Architecture

```
NewsAPI / OpenWeatherMap API
        ↓
    main.py (API calls → validate (error handling) → deduplicate → load)
        ↓
  Google BigQuery
        ↓
  Discord (alerts)
```

---

## Free Tier Services

| Service                                                                                                                   | Usage                    | Free Tier Limit                   |
| ------------------------------------------------------------------------------------------------------------------------- | ------------------------ | --------------------------------- |
| [NewsAPI](https://newsapi.org)                                                                                            | Fetch top headlines      | 100 requests/day                  |
| [OpenWeatherMap](https://openweathermap.org/api)                                                                          | Fetch 5-day forecast     | 1,000 requests/day                |
| [Google BigQuery](https://cloud.google.com/bigquery/pricing)                                                              | Store and query data     | 10 GB storage, 1 TB queries/month |
| [GitHub Actions](https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions) | Run pipeline on schedule | 2,000 minutes/month               |
| [Discord Webhooks](https://discord.com/developers/docs/resources/webhook)                                                 | Pipeline alerts          | Free                              |

---

## Setup

### Prerequisites

- Python 3.13+
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (`gcloud`)
- A Google Cloud project with BigQuery enabled
- A NewsAPI account
- An OpenWeatherMap account
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

---

## BigQuery Schema

See [`schema.sql`](./schema.sql) for full table definitions.

**`news_pipeline.top_headlines`**
| Column | Type | Description |
|---|---|---|
| `article_id` | STRING | SHA-256 hash of article URL (primary key) |
| `source_id` | STRING | News source identifier |
| `source_name` | STRING | News source display name |
| `authors` | ARRAY\<STRING\> | Article authors |
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

---

## Automated Scheduling

The pipeline runs daily at 9AM Eastern via GitHub Actions. To change the schedule, update the cron value in [`.github/workflows/pipeline.yml`](./.github/workflows/pipeline.yml).

To run manually, go to the **Actions** tab in GitHub → **Data Pipeline** → **Run workflow**.

### GitHub Secrets Required

| Secret                 | Description                    |
| ---------------------- | ------------------------------ |
| `NEWS_API_KEY`         | NewsAPI key                    |
| `OPEN_WEATHER_API_KEY` | OpenWeatherMap API key         |
| `DISCORD_WEBHOOK_URL`  | Discord webhook URL for alerts |

---

## Error Handling

- **Timeouts** — caught and reported to Discord
- **HTTP errors** — status code reported to Discord
- **Schema changes** — detected if expected top-level keys are missing from API response
- **Data validation** — records missing required fields or containing invalid values are skipped and logged before insert
- **Duplicate prevention** — records already in BigQuery are filtered out before each insert using SHA-256 hashed primary keys
