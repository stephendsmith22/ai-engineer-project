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

api_key = os.getenv("news_api_key")
discord_webhook_url = os.getenv("discord_webhook_url")
gcp_project_id = os.getenv("gcp_project_id")

DATASET_ID = "news_pipeline"
TABLE_ID = "top_headlines"

news_date = "2026-03-31"

news_url = (
    "https://newsapi.org/v2/top-headlines?"
    "country=us&"
    f"from={news_date}&"
    "sortBy=popularity&"
    f"apiKey={api_key}"
)


def send_discord_alert(message: str):
    if not discord_webhook_url:
        logger.warning("Discord webhook URL not configured")
        return
    try:
        requests.post(discord_webhook_url, json={"content": message}, timeout=10)
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")


def ensure_dataset_and_table(client: bigquery.Client):
    dataset_ref = bigquery.Dataset(f"{gcp_project_id}.{DATASET_ID}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info(f"Dataset '{DATASET_ID}' ready")

    schema = [
        bigquery.SchemaField("source_id", "STRING"),
        bigquery.SchemaField("source_name", "STRING"),
        bigquery.SchemaField("authors", "STRING", mode="REPEATED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("url_to_image", "STRING"),
        bigquery.SchemaField("published_at", "TIMESTAMP"),
        bigquery.SchemaField("content", "STRING"),
    ]
    table_ref = bigquery.Table(
        f"{gcp_project_id}.{DATASET_ID}.{TABLE_ID}", schema=schema
    )
    try:
        existing = client.get_table(table_ref)
        if not existing.schema:
            existing.schema = schema
            client.update_table(existing, ["schema"])
            logger.info(f"Table '{TABLE_ID}' schema updated")
    except Exception:
        client.create_table(table_ref)
    logger.info(f"Table '{TABLE_ID}' ready")


def load_to_bigquery(records: list):
    client = bigquery.Client(project=gcp_project_id)
    ensure_dataset_and_table(client)

    table_ref = f"{gcp_project_id}.{DATASET_ID}.{TABLE_ID}"
    errors = client.insert_rows_json(table_ref, records)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    logger.info(f"Loaded {len(records)} records to {table_ref}")


def fetch_news():
    transformed_records = []

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
            record = {
                "source_id": article.get("source", {}).get("id", ""),
                "source_name": article.get("source", {}).get("name", ""),
                "authors": [a.strip() for a in (article.get("author") or "").split(",") if a.strip()],
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "url": article.get("url", ""),
                "url_to_image": article.get("urlToImage", ""),
                "published_at": article.get("publishedAt"),
                "content": article.get("content", ""),
            }
            transformed_records.append(record)

        load_to_bigquery(transformed_records)
        send_discord_alert(
            f"Pipeline succeeded: Loaded {len(transformed_records)} news articles"
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


if __name__ == "__main__":
    fetch_news()
