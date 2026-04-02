import hashlib
import datetime
import os
import logging
import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from common import send_discord_alert, ensure_dataset_and_table, load_to_bigquery
from schemas import NEWS_SCHEMA, WEATHER_SCHEMA, LOGS_SCHEMA, RUNS_SCHEMA

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

news_api_key = os.getenv("news_api_key")
weather_api_key = os.getenv("open_weather_api_key")
gcp_project_id = os.getenv("gcp_project_id")

NEWS_DATASET_ID = "news_pipeline"
NEWS_TABLE_ID = "top_headlines"

WEATHER_DATASET_ID = "weather_pipeline"
WEATHER_TABLE_ID = "weather_forecast"

LOGS_DATASET_ID = "pipeline_logs"
LOGS_TABLE_ID = "api_errors"
RUNS_TABLE_ID = "pipeline_runs"

# grab today's top headlines
now = datetime.datetime.now()
city = "Miami"


def validate_news_record(record: dict) -> bool:
    if not record.get("url"):
        logger.warning(
            f"Skipping article missing URL: {record.get('title', 'unknown')}"
        )
        return False
    if not record.get("title"):
        logger.warning(f"Skipping article missing title: {record.get('url')}")
        return False
    if not record.get("published_at"):
        logger.warning(f"Skipping article missing published_at: {record.get('url')}")
        return False
    return True


def validate_weather_record(record: dict) -> bool:
    if not record.get("city"):
        logger.warning("Skipping weather record missing city")
        return False
    if not record.get("datetime"):
        logger.warning(
            f"Skipping weather record missing datetime for city: {record.get('city')}"
        )
        return False
    numeric_fields = ["temperature", "feels_like", "temp_min", "temp_max", "wind_speed"]
    for field in numeric_fields:
        val = record.get(field)
        if val == "" or val is None:
            logger.warning(
                f"Skipping weather record — missing numeric field '{field}' for {record.get('city')} {record.get('datetime')}"
            )
            return False
        try:
            float(val)
        except (TypeError, ValueError):
            logger.warning(
                f"Skipping weather record — invalid numeric value for '{field}': {val}"
            )
            return False
    return True


def log_error_to_bigquery(api_source: str, error_type: str, error_message: str):
    try:
        client = bigquery.Client(project=gcp_project_id)
        ensure_dataset_and_table(
            client, LOGS_DATASET_ID, LOGS_TABLE_ID, LOGS_SCHEMA, logger
        )
        table_ref = f"{gcp_project_id}.{LOGS_DATASET_ID}.{LOGS_TABLE_ID}"
        record = [
            {
                "log_id": hashlib.sha256(
                    f"{api_source}{error_type}{error_message}{now}".encode()
                ).hexdigest(),
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                "api_source": api_source,
                "error_type": error_type,
                "error_message": error_message,
            }
        ]
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        client.load_table_from_json(record, table_ref, job_config=job_config).result()
    except Exception as e:
        logger.error(f"Failed to log error to BigQuery: {e}")


def log_run_to_bigquery(
    api_source: str, records_fetched: int, records_loaded: int, errors: int, status: str, pipeline_run_id: str = ""
):
    try:
        client = bigquery.Client(project=gcp_project_id)
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        ensure_dataset_and_table(
            client, LOGS_DATASET_ID, RUNS_TABLE_ID, RUNS_SCHEMA, logger
        )
        table_ref = f"{gcp_project_id}.{LOGS_DATASET_ID}.{RUNS_TABLE_ID}"
        record = [
            {
                "run_id": hashlib.sha256(
                    f"{api_source}{timestamp}".encode()
                ).hexdigest(),
                "timestamp": timestamp,
                "pipeline_run_id": pipeline_run_id,
                "api_source": api_source,
                "records_fetched": records_fetched,
                "records_loaded": records_loaded,
                "errors": errors,
                "status": status,
            }
        ]
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        client.load_table_from_json(record, table_ref, job_config=job_config).result()
    except Exception as e:
        logger.error(f"Failed to log pipeline run to BigQuery: {e}")


def fetch_news(pipeline_run_id: str):
    transformed_records = []

    news_url = f"https://newsapi.org/v2/top-headlines?country=us&apiKey={news_api_key}"

    try:
        response = requests.get(news_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "articles" not in data:
            msg = "API format changed - missing 'articles' key"
            logger.error(msg)
            send_discord_alert(f"🚨 **NewsAPI — Schema Change**> {msg}", logger)
            return

        articles = data.get("articles", [])
        records_fetched = len(articles)

        for article in articles:
            url = article.get("url", "")
            record = {
                "article_id": hashlib.sha256(url.encode()).hexdigest(),
                "source_id": article.get("source", {}).get("id", ""),
                "source_name": article.get("source", {}).get("name", ""),
                "authors": [
                    a.strip()
                    for a in (article.get("author") or "").split(",")
                    if a.strip()
                ],
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "url": url,
                "url_to_image": article.get("urlToImage", ""),
                "published_at": article.get("publishedAt"),
                "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "content": article.get("content", ""),
            }
            if validate_news_record(record):
                transformed_records.append(record)

        validation_errors = records_fetched - len(transformed_records)
        records_loaded = (
            load_to_bigquery(
                transformed_records,
                NEWS_DATASET_ID,
                NEWS_TABLE_ID,
                NEWS_SCHEMA,
                dedup_field="article_id",
                logger=logger,
            )
            or 0
        )

        status = "SUCCESS" if validation_errors == 0 else "PARTIAL"
        log_run_to_bigquery(
            "NewsAPI", records_fetched, records_loaded, validation_errors, status, pipeline_run_id
        )

    except requests.exceptions.Timeout:
        msg = "NewsAPI request timed out"
        logger.error(msg)
        log_error_to_bigquery("NewsAPI", "Timeout", msg)
        log_run_to_bigquery("NewsAPI", 0, 0, 1, "FAILED", pipeline_run_id)
        send_discord_alert(f"❌ **News Pipeline Error**> {msg}", logger)

    except requests.exceptions.HTTPError as e:
        msg = f"NewsAPI HTTP error: {e.response.status_code}"
        logger.error(msg)
        log_error_to_bigquery("NewsAPI", "HTTPError", msg)
        log_run_to_bigquery("NewsAPI", 0, 0, 1, "FAILED", pipeline_run_id)
        send_discord_alert(f"❌ **News Pipeline Error**> {msg}", logger)

    except Exception as e:
        msg = f"Pipeline failed: {str(e)}"
        logger.error(msg)
        log_error_to_bigquery("NewsAPI", "Exception", msg)
        log_run_to_bigquery("NewsAPI", 0, 0, 1, "FAILED", pipeline_run_id)
        send_discord_alert(f"❌ **News Pipeline Error**> {msg}", logger)


def fetch_weather(pipeline_run_id: str):
    transformed_records = []
    weather_forecast_count = 40  # get next 5 days of 3-hour forecasts (8 per day)
    city = "Miami"

    weather_url = (
        "https://api.openweathermap.org/data/2.5/forecast?"
        f"q={city}&"
        f"units=imperial&"
        f"cnt={weather_forecast_count}&"
        f"appid={weather_api_key}"
    )

    try:
        # fetch weather data for the city
        response = requests.get(weather_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "list" not in data:
            msg = "API format changed - missing 'list' key"
            logger.error(msg)
            send_discord_alert(f"🚨 **WeatherAPI — Schema Change**> {msg}", logger)
            return

        forecasts = data.get("list", [])
        records_fetched = len(forecasts)

        for forecast in forecasts:
            city = data.get("city", {}).get("name", "")
            dt_txt = forecast.get("dt_txt", "")
            forecast_id = hashlib.sha256(f"{city}_{dt_txt}".encode()).hexdigest()
            record = {
                "forecast_id": forecast_id,
                "city": city,
                "datetime": dt_txt,
                "temperature": forecast.get("main", {}).get("temp", ""),
                "feels_like": forecast.get("main", {}).get("feels_like", ""),
                "temp_min": forecast.get("main", {}).get("temp_min", ""),
                "temp_max": forecast.get("main", {}).get("temp_max", ""),
                "humidity": forecast.get("main", {}).get("humidity", ""),
                "weather_main": forecast.get("weather", [{}])[0].get("main", ""),
                "weather_description": forecast.get("weather", [{}])[0].get(
                    "description", ""
                ),
                "wind_speed": forecast.get("wind", {}).get("speed", ""),
                "visibility": forecast.get("visibility", ""),
                "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if validate_weather_record(record):
                transformed_records.append(record)

        validation_errors = records_fetched - len(transformed_records)
        records_loaded = (
            load_to_bigquery(
                transformed_records,
                WEATHER_DATASET_ID,
                WEATHER_TABLE_ID,
                WEATHER_SCHEMA,
                dedup_field="forecast_id",
                logger=logger,
            )
            or 0
        )

        status = "SUCCESS" if validation_errors == 0 else "PARTIAL"
        log_run_to_bigquery(
            "WeatherAPI", records_fetched, records_loaded, validation_errors, status, pipeline_run_id
        )

    except requests.exceptions.Timeout:
        msg = "WeatherAPI request timed out"
        logger.error(msg)
        log_error_to_bigquery("WeatherAPI", "Timeout", msg)
        log_run_to_bigquery("WeatherAPI", 0, 0, 1, "FAILED", pipeline_run_id)
        send_discord_alert(f"❌ **Weather Pipeline Error**> {msg}", logger)

    except requests.exceptions.HTTPError as e:
        msg = f"WeatherAPI HTTP error: {e.response.status_code}"
        logger.error(msg)
        log_error_to_bigquery("WeatherAPI", "HTTPError", msg)
        log_run_to_bigquery("WeatherAPI", 0, 0, 1, "FAILED", pipeline_run_id)
        send_discord_alert(f"❌ **Weather Pipeline Error**> {msg}", logger)

    except Exception as e:
        msg = f"Weather Pipeline failed: {str(e)}"
        logger.error(msg)
        log_error_to_bigquery("WeatherAPI", "Exception", msg)
        log_run_to_bigquery("WeatherAPI", 0, 0, 1, "FAILED", pipeline_run_id)
        send_discord_alert(f"❌ **Weather Pipeline Error**> {msg}", logger)


if __name__ == "__main__":
    pipeline_run_id = hashlib.sha256(datetime.datetime.now(datetime.timezone.utc).isoformat().encode()).hexdigest()
    fetch_news(pipeline_run_id)
    fetch_weather(pipeline_run_id)
