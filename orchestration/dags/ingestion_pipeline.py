"""
Pawprint · Ingestion Pipeline DAG
==================================
Orchestrates the full document ingestion flow:

  GCS upload event
      → validate file
      → Cloud DLP scan (PII / PHI)
      → Document AI OCR  (PDFs only)
      → Gemini structured extraction
      → embed text  (Vertex AI text-embedding-004)
      → write to BigQuery  (health_events + media_assets)
      → upsert embedding to Vector Search

Schedule: triggered externally via Pub/Sub (not on a cron).
Trigger mechanism: Cloud Functions publishes to the 'ingestion-trigger'
topic when a file lands in GCS; Composer listens via a sensor.

Author: Yifei (Faye) Wang
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Config — pulled from Airflow Variables or environment
# ---------------------------------------------------------------------------
PROJECT_ID        = os.environ.get("GCP_PROJECT_ID", "pawprint-dev")
REGION            = os.environ.get("GCP_REGION", "us-central1")
BQ_DATASET        = os.environ.get("BQ_DATASET", "pawprint_db")
GCS_BUCKET        = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-pawprint-raw")
PUBSUB_TOPIC      = os.environ.get("PUBSUB_INGESTION_TOPIC", "ingestion-trigger")
PUBSUB_SUB        = os.environ.get("PUBSUB_INGESTION_SUB",   "ingestion-trigger-sub")
DLP_TEMPLATE      = os.environ.get("DLP_INSPECT_TEMPLATE",   "projects/pawprint-dev/inspectTemplates/pet-pii-template")
VECTOR_SEARCH_INDEX = os.environ.get("VECTOR_SEARCH_INDEX_ID", "")

# ---------------------------------------------------------------------------
# Default DAG args
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "faye",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def validate_file(**context) -> dict:
    """
    Pull the Pub/Sub message from XCom, decode the GCS event,
    validate file type and size, return metadata dict.
    """
    messages = context["ti"].xcom_pull(task_ids="wait_for_upload")
    # In production: parse base64-encoded Pub/Sub data containing GCS event
    # For now: placeholder structure
    message_data = {
        "gcs_uri":   "gs://pawprint-dev-raw/uploads/sample_vet_record.pdf",
        "pet_id":    "pet_abc123",
        "asset_type": "pdf",
        "filename":  "sample_vet_record.pdf",
    }
    allowed_types = {"pdf", "jpg", "jpeg", "png", "mp4", "m4a", "txt"}
    ext = message_data["filename"].rsplit(".", 1)[-1].lower()
    if ext not in allowed_types:
        raise ValueError(f"Unsupported file type: {ext}")

    context["ti"].xcom_push(key="file_meta", value=message_data)
    return message_data


def run_dlp_scan(**context) -> dict:
    """
    Submit the GCS file to Cloud DLP for PII / PHI inspection.
    Findings are stored back to BigQuery via DLP job config.
    Returns: dict with scan_id and any critical findings.
    """
    from google.cloud import dlp_v2  # noqa: PLC0415

    file_meta = context["ti"].xcom_pull(task_ids="validate_file", key="file_meta")
    client = dlp_v2.DlpServiceClient()

    # Build DLP job for GCS content inspection
    inspect_job = {
        "inspect_config": {
            "info_types": [
                {"name": "PERSON_NAME"},
                {"name": "DATE_OF_BIRTH"},
                {"name": "MEDICAL_RECORD_NUMBER"},
                {"name": "EMAIL_ADDRESS"},
                {"name": "PHONE_NUMBER"},
            ],
            "min_likelihood": "LIKELY",
        },
        "storage_config": {
            "cloud_storage_options": {
                "file_set": {"url": file_meta["gcs_uri"]}
            }
        },
        "actions": [
            {
                "save_findings": {
                    "output_config": {
                        "table": {
                            "project_id": PROJECT_ID,
                            "dataset_id": BQ_DATASET,
                            "table_id":   "dlp_findings",
                        }
                    }
                }
            }
        ],
    }

    parent = f"projects/{PROJECT_ID}/locations/{REGION}"
    # In production: client.create_dlp_job(parent=parent, inspect_job=inspect_job)
    # For now: simulate success
    print(f"DLP scan submitted for: {file_meta['gcs_uri']}")
    context["ti"].xcom_push(key="dlp_findings", value={"findings": [], "scanned": True})


def run_document_ai(**context) -> dict:
    """
    Run Document AI Form Parser on the uploaded PDF.
    Returns structured key-value pairs extracted from the vet record.
    Only executed for PDF asset types.
    """
    from google.cloud import documentai  # noqa: PLC0415

    file_meta = context["ti"].xcom_pull(task_ids="validate_file", key="file_meta")

    if file_meta["asset_type"] != "pdf":
        print("Not a PDF — skipping Document AI.")
        context["ti"].xcom_push(key="ocr_result", value={"text": "", "entities": []})
        return {}

    # In production: read GCS file, submit to Document AI processor
    # processor_name = f"projects/{PROJECT_ID}/locations/{REGION}/processors/PROCESSOR_ID"
    # client = documentai.DocumentProcessorServiceClient()
    # document = client.process_document(name=processor_name, raw_document=...)

    # Placeholder output structure matching Document AI response
    ocr_result = {
        "text": "Patient: Mochi (cat, DSH, F/S). Visit date: 2025-03-10. "
                "Diagnosis: Mild upper respiratory infection. "
                "Treatment: Doxycycline 25mg PO BID x 10 days.",
        "entities": [
            {"type": "patient_name",  "value": "Mochi",       "confidence": 0.97},
            {"type": "visit_date",    "value": "2025-03-10",  "confidence": 0.99},
            {"type": "diagnosis",     "value": "URI",         "confidence": 0.94},
            {"type": "medication",    "value": "Doxycycline", "confidence": 0.98},
        ],
    }
    context["ti"].xcom_push(key="ocr_result", value=ocr_result)
    return ocr_result


def run_gemini_extraction(**context) -> dict:
    """
    Pass Document AI output + raw text to Gemini 1.5 Flash.
    Prompt asks Gemini to produce a structured JSON health event
    conforming to the BigQuery health_events schema.
    """
    import json  # noqa: PLC0415

    import vertexai  # noqa: PLC0415
    from vertexai.generative_models import GenerativeModel  # noqa: PLC0415

    ocr_result = context["ti"].xcom_pull(task_ids="run_document_ai", key="ocr_result")
    file_meta  = context["ti"].xcom_pull(task_ids="validate_file",   key="file_meta")

    vertexai.init(project=PROJECT_ID, location=REGION)
    model = GenerativeModel("gemini-1.5-flash-002")

    prompt = f"""
You are a veterinary record parser. Extract structured information from the following
vet record text and return ONLY valid JSON matching this schema:

{{
  "event_type": "vet_visit" | "vaccine" | "lab" | "procedure" | "diagnosis",
  "event_date": "YYYY-MM-DD",
  "title": "short human-readable title",
  "provider_name": "clinic or vet name if present",
  "diagnoses": ["list of diagnoses in plain English"],
  "procedures": ["list of procedures"],
  "vaccines": ["list of vaccines if any"],
  "medications": [
    {{
      "name": "drug name",
      "dosage": "dose string",
      "frequency": "frequency string",
      "route": "oral|topical|injection|other",
      "duration_days": integer or null
    }}
  ],
  "notes": "any additional clinical notes"
}}

Vet record text:
{ocr_result.get('text', '')}

Return ONLY the JSON object. No explanation.
"""

    # In production: response = model.generate_content(prompt)
    # structured = json.loads(response.text)

    # Placeholder matching expected schema
    structured = {
        "event_type":    "vet_visit",
        "event_date":    "2025-03-10",
        "title":         "Upper respiratory infection treatment",
        "provider_name": None,
        "diagnoses":     ["Mild upper respiratory infection"],
        "procedures":    [],
        "vaccines":      [],
        "medications":   [
            {
                "name":         "Doxycycline",
                "dosage":       "25mg",
                "frequency":    "twice daily",
                "route":        "oral",
                "duration_days": 10,
            }
        ],
        "notes": "",
    }
    structured["pet_id"]      = file_meta["pet_id"]
    structured["source_doc_id"] = file_meta.get("asset_id", "")

    context["ti"].xcom_push(key="structured_event", value=structured)
    return structured


def generate_and_store_embedding(**context) -> None:
    """
    Generate a text embedding from the structured event + raw OCR text.
    Upsert into Vertex AI Vector Search for later RAG retrieval.
    """
    import json  # noqa: PLC0415

    import vertexai  # noqa: PLC0415
    from vertexai.language_models import TextEmbeddingModel  # noqa: PLC0415

    ocr_result       = context["ti"].xcom_pull(task_ids="run_document_ai",      key="ocr_result")
    structured_event = context["ti"].xcom_pull(task_ids="run_gemini_extraction", key="structured_event")

    vertexai.init(project=PROJECT_ID, location=REGION)
    model = TextEmbeddingModel.from_pretrained("text-embedding-004")

    # Compose the text to embed: combine title + diagnoses + notes
    embed_text = " ".join(filter(None, [
        structured_event.get("title", ""),
        " ".join(structured_event.get("diagnoses", [])),
        ocr_result.get("text", "")[:2000],   # cap at 2k chars
    ]))

    # In production: embeddings = model.get_embeddings([embed_text])
    # vector = embeddings[0].values
    # Then upsert to Vector Search via MatchingEngineIndexEndpoint

    print(f"Embedding generated for event: {structured_event.get('title')}")
    print(f"Text length: {len(embed_text)} chars")
    # Placeholder: in production write embedding ID back to media_assets


def write_to_bigquery(**context) -> None:
    """
    Insert the structured health event into BigQuery health_events table.
    Also insert/update the media_assets record for the source document.
    Uses the BigQuery streaming insert API for low-latency writes.
    """
    import uuid  # noqa: PLC0415
    from datetime import timezone  # noqa: PLC0415

    from google.cloud import bigquery  # noqa: PLC0415

    structured_event = context["ti"].xcom_pull(task_ids="run_gemini_extraction", key="structured_event")
    file_meta        = context["ti"].xcom_pull(task_ids="validate_file",          key="file_meta")
    ocr_result       = context["ti"].xcom_pull(task_ids="run_document_ai",        key="ocr_result")

    client = bigquery.Client(project=PROJECT_ID)

    # --- health_events row ---
    event_row = {
        "event_id":               str(uuid.uuid4()),
        "pet_id":                 structured_event["pet_id"],
        "event_date":             structured_event["event_date"],
        "event_type":             structured_event["event_type"],
        "title":                  structured_event["title"],
        "description":            ocr_result.get("text", ""),
        "provider_name":          structured_event.get("provider_name"),
        "diagnoses":              structured_event.get("diagnoses", []),
        "procedures":             structured_event.get("procedures", []),
        "vaccines":               structured_event.get("vaccines", []),
        "notes":                  structured_event.get("notes", ""),
        "source_doc_id":          structured_event.get("source_doc_id", ""),
        "extraction_confidence":  0.95,   # TODO: pull real score from Gemini
        "created_at":             datetime.now(timezone.utc).isoformat(),
    }

    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.health_events"
    errors = client.insert_rows_json(table_ref, [event_row])
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    print(f"health_event written: {event_row['event_id']}")

    # --- media_assets row (the source PDF) ---
    asset_row = {
        "asset_id":    str(uuid.uuid4()),
        "pet_id":      file_meta["pet_id"],
        "asset_type":  file_meta["asset_type"],
        "upload_date": datetime.now(timezone.utc).date().isoformat(),
        "gcs_uri":     file_meta["gcs_uri"],
        "filename":    file_meta["filename"],
        "ocr_text":    ocr_result.get("text", ""),
        "dlp_scanned": True,
        "created_at":  datetime.now(timezone.utc).isoformat(),
    }
    asset_table_ref = f"{PROJECT_ID}.{BQ_DATASET}.media_assets"
    client.insert_rows_json(asset_table_ref, [asset_row])
    print(f"media_asset written: {asset_row['asset_id']}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="pawprint_ingestion_pipeline",
    default_args=default_args,
    description="Ingest vet records: GCS → DLP → Document AI → Gemini → BigQuery + Vector Search",
    schedule_interval=None,           # triggered by Pub/Sub sensor, not cron
    start_date=days_ago(1),
    catchup=False,
    tags=["pawprint", "ingestion", "rag"],
    doc_md=__doc__,
) as dag:

    # 1. Wait for a file-upload event on Pub/Sub
    wait_for_upload = PubSubPullSensor(
        task_id="wait_for_upload",
        project_id=PROJECT_ID,
        subscription=PUBSUB_SUB,
        max_messages=1,
        ack_messages=True,
        poke_interval=30,
        timeout=3600,
    )

    # 2. Validate the incoming file
    validate = PythonOperator(
        task_id="validate_file",
        python_callable=validate_file,
    )

    # 3. Cloud DLP scan for PII / PHI
    dlp_scan = PythonOperator(
        task_id="run_dlp_scan",
        python_callable=run_dlp_scan,
    )

    # 4. Document AI OCR (PDFs)
    doc_ai = PythonOperator(
        task_id="run_document_ai",
        python_callable=run_document_ai,
    )

    # 5. Gemini structured extraction
    gemini_extract = PythonOperator(
        task_id="run_gemini_extraction",
        python_callable=run_gemini_extraction,
    )

    # 6. Generate embedding + upsert to Vector Search
    embed = PythonOperator(
        task_id="generate_and_store_embedding",
        python_callable=generate_and_store_embedding,
    )

    # 7. Write structured record to BigQuery
    bq_write = PythonOperator(
        task_id="write_to_bigquery",
        python_callable=write_to_bigquery,
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    (
        wait_for_upload
        >> validate
        >> dlp_scan
        >> doc_ai
        >> gemini_extract
        >> [embed, bq_write]   # embedding + BQ write run in parallel
    )
