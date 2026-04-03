import os
from dotenv import load_dotenv
import requests
from google.cloud import bigquery

load_dotenv()

gcp_project_id = os.getenv("gcp_project_id")
discord_webhook_url = os.getenv("discord_webhook_url")


def send_discord_alert(message: str, logger):
    if not discord_webhook_url:
        logger.warning("Discord webhook URL not configured")
        return
    try:
        requests.post(discord_webhook_url, json={"content": message}, timeout=10)
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")


def ensure_dataset_and_table(
    client: bigquery.Client, dataset_id: str, table_id: str, schema: list, logger
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
    logger.info(f"Table '{table_id}' ready\n")


def load_to_bigquery(
    records: list,
    dataset_id: str,
    table_id: str,
    schema: list,
    dedup_field: str = None,
    logger=None,
):
    client = bigquery.Client(project=gcp_project_id)
    ensure_dataset_and_table(client, dataset_id, table_id, schema, logger)

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
                f"⚠️ **{table_id}** — No new records to insert, all already exist",
                logger,
            )
            return 0
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
    send_discord_alert(
        f"✅ **{table_id}** — Inserted **{len(records)}** new records", logger
    )
    return len(records)
