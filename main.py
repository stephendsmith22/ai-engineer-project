import hashlib
import datetime
import os
import logging
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

news_api_key = os.getenv("news_api_key")
weather_api_key = os.getenv("open_weather_api_key")
discord_webhook_url = os.getenv("discord_webhook_url")
gcp_project_id = os.getenv("gcp_project_id")

NEWS_DATASET_ID = "news_pipeline"
NEWS_TABLE_ID = "news_articles"

WEATHER_DATASET_ID = "weather_pipeline"
WEATHER_TABLE_ID = "weather_forecast"

# grab today's news articles
now = datetime.datetime.now()
news_date = now.strftime("%Y-%m-%d")
city = "Miami"


def send_discord_alert(message: str):
    if not discord_webhook_url:
        logger.warning("Discord webhook URL not configured")
        return
    try:
        requests.post(discord_webhook_url, json={"content": message}, timeout=10)
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")


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


def ensure_dataset_and_table(
    client: bigquery.Client, dataset_id: str, table_id: str, schema: list
):
    dataset_ref = bigquery.Dataset(f"{gcp_project_id}.{dataset_id}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info(f"Dataset '{dataset_id}' ready")

    table_ref = bigquery.Table(
        f"{gcp_project_id}.{dataset_id}.{table_id}", schema=schema
    )
    try:
        existing = client.get_table(table_ref)
        if not existing.schema:
            existing.schema = schema
            client.update_table(existing, ["schema"])
            logger.info(f"Table '{table_id}' schema updated")
    except Exception:
        client.create_table(table_ref)
    logger.info(f"Table '{table_id}' ready")


def load_to_bigquery(
    records: list, dataset_id: str, table_id: str, schema: list, dedup_field: str = None
):
    client = bigquery.Client(project=gcp_project_id)
    ensure_dataset_and_table(client, dataset_id, table_id, schema)

    table_ref = f"{gcp_project_id}.{dataset_id}.{table_id}"

    if dedup_field:
        incoming_ids = [r[dedup_field] for r in records]
        existing = client.query(
            f"SELECT {dedup_field} FROM `{table_ref}` WHERE {dedup_field} IN UNNEST(@ids)",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("ids", "STRING", incoming_ids)
                ]
            ),
        ).result()
        existing_ids = {row[dedup_field] for row in existing}
        records = [r for r in records if r[dedup_field] not in existing_ids]
        if not records:
            logger.info("No new records to insert — all already exist")
            send_discord_alert(
                f"{table_id}: No new records to insert — all already exist"
            )
            return
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    else:
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_json(records, table_ref, job_config=job_config)
    job.result()

    logger.info(f"Loaded {len(records)} records to {table_ref}")
    send_discord_alert(f"{table_id}: Inserted {len(records)} new records")


def fetch_news():
    transformed_records = []

    news_url = (
        "https://newsapi.org/v2/everything?"
        "q=news&"
        f"from={news_date}&"
        f"to={news_date}&"
        "language=en&"
        "sortBy=popularity&"
        f"apiKey={news_api_key}"
    )

    try:
        response = requests.get(news_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "articles" not in data:
            msg = "API format changed - missing 'articles' key"
            logger.error(msg)
            send_discord_alert(f"NewsAPI schema change detected: {msg}")
            return

        for article in data.get("articles", []):
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
                "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "content": article.get("content", ""),
            }
            transformed_records.append(record)

        load_to_bigquery(
            transformed_records,
            NEWS_DATASET_ID,
            NEWS_TABLE_ID,
            NEWS_SCHEMA,
            dedup_field="article_id",
        )

    except requests.exceptions.Timeout:
        msg = "NewsAPI request timed out"
        logger.error(msg)
        send_discord_alert(f"Pipeline error: {msg}")

    except requests.exceptions.HTTPError as e:
        msg = f"NewsAPI HTTP error: {e.response.status_code}"
        logger.error(msg)
        send_discord_alert(f"Pipeline error: {msg}")

    except Exception as e:
        msg = f"Pipeline failed: {str(e)}"
        logger.error(msg)
        send_discord_alert(msg)


def fetch_weather():
    transformed_records = []
    weather_forecast_count = 40  # get next 5 days of 3-hour forecasts (8 per day)

    weather_url = (
        "https://api.openweathermap.org/data/2.5/forecast?"
        f"q=Miami&"
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
            send_discord_alert(f"WeatherAPI schema change detected: {msg}")
            return

        for forecast in data.get("list", []):
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
                "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            transformed_records.append(record)

        load_to_bigquery(
            transformed_records,
            WEATHER_DATASET_ID,
            WEATHER_TABLE_ID,
            WEATHER_SCHEMA,
            dedup_field="forecast_id",
        )

    except requests.exceptions.Timeout:
        msg = "WeatherAPI request timed out"
        logger.error(msg)
        send_discord_alert(f"Pipeline error: {msg}")

    except requests.exceptions.HTTPError as e:
        msg = f"WeatherAPI HTTP error: {e.response.status_code}"
        logger.error(msg)
        send_discord_alert(f"Pipeline error: {msg}")

    except Exception as e:
        msg = f"Weather Pipeline failed: {str(e)}"
        logger.error(msg)
        send_discord_alert(msg)


if __name__ == "__main__":
    fetch_news()
    fetch_weather()
