"""
Pawprint · GCS Upload Handler
==============================
Usage:
    python upload.py <file_path> --pet-id <pet_id>

Examples:
    python upload.py ~/Downloads/vet_record.pdf --pet-id pet_mochi_001
    python upload.py ~/Photos/mochi.jpg --pet-id pet_mochi_001

What it does:
    1. Validates file type and size
    2. Uploads file to GCS (raw landing bucket)
    3. Publishes a Pub/Sub message to trigger the ingestion pipeline
    4. Prints a summary of what happened
"""

import argparse
import json
import mimetypes
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import pubsub_v1, storage

# ── Load environment variables from .env ──────────────────
load_dotenv()

PROJECT_ID   = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET   = os.getenv("GCS_BUCKET")
PUBSUB_TOPIC = os.getenv("PUBSUB_INGESTION_TOPIC", "ingestion-trigger")

# ── Allowed file types ────────────────────────────────────
ALLOWED_EXTENSIONS = {
    ".pdf":  "application/pdf",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".txt":  "text/plain",
    ".mp4":  "video/mp4",
}
MAX_FILE_SIZE_MB = 50


def validate_file(file_path: Path) -> str:
    """Check file exists, type is allowed, size is within limit."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    ext = file_path.suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file type: '{ext}'\n"
            f"Allowed: {', '.join(ALLOWED_EXTENSIONS.keys())}"
        )

    size_mb = file_path.stat().st_size / (1024 * 1024)
    if size_mb > MAX_FILE_SIZE_MB:
        raise ValueError(f"File too large: {size_mb:.1f}MB (max {MAX_FILE_SIZE_MB}MB)")

    print(f"  ✓ File valid: {file_path.name} ({size_mb:.2f}MB, {ext})")
    return ALLOWED_EXTENSIONS[ext]


def upload_to_gcs(file_path: Path, pet_id: str, asset_id: str, mime_type: str) -> str:
    """Upload file to GCS and return the gs:// URI."""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)

    # Organised path: uploads/{pet_id}/{date}/{asset_id}/{filename}
    date_prefix = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    blob_name   = f"uploads/{pet_id}/{date_prefix}/{asset_id}/{file_path.name}"
    blob        = bucket.blob(blob_name)

    print(f"  ↑ Uploading to gs://{GCS_BUCKET}/{blob_name} ...")
    blob.upload_from_filename(str(file_path), content_type=mime_type)

    gcs_uri = f"gs://{GCS_BUCKET}/{blob_name}"
    print(f"  ✓ Upload complete: {gcs_uri}")
    return gcs_uri


def publish_to_pubsub(
    pet_id: str,
    asset_id: str,
    gcs_uri: str,
    filename: str,
    asset_type: str,
    mime_type: str,
) -> str:
    """Publish ingestion trigger message to Pub/Sub. Returns message ID."""
    publisher   = pubsub_v1.PublisherClient()
    topic_path  = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

    message_payload = {
        "asset_id":   asset_id,
        "pet_id":     pet_id,
        "gcs_uri":    gcs_uri,
        "filename":   filename,
        "asset_type": asset_type,       # 'pdf' | 'photo' | 'video'
        "mime_type":  mime_type,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "source":     "cli_upload",
    }

    # Pub/Sub messages must be bytes
    data = json.dumps(message_payload).encode("utf-8")

    print(f"  → Publishing to Pub/Sub topic '{PUBSUB_TOPIC}' ...")
    future     = publisher.publish(topic_path, data=data)
    message_id = future.result()          # blocks until confirmed
    print(f"  ✓ Message published: {message_id}")
    return message_id


def infer_asset_type(file_path: Path) -> str:
    """Map file extension to asset_type string."""
    ext = file_path.suffix.lower()
    if ext == ".pdf":
        return "pdf"
    if ext in {".jpg", ".jpeg", ".png"}:
        return "photo"
    if ext in {".mp4", ".mov"}:
        return "video"
    return "other"


def main():
    parser = argparse.ArgumentParser(
        description="Upload a file to Pawprint GCS and trigger the ingestion pipeline."
    )
    parser.add_argument(
        "file",
        help="Path to the file to upload (PDF, JPG, PNG, TXT, MP4)"
    )
    parser.add_argument(
        "--pet-id",
        required=True,
        help="Pet identifier, e.g. pet_mochi_001"
    )
    args = parser.parse_args()

    file_path = Path(args.file).expanduser().resolve()
    pet_id    = args.pet_id
    asset_id  = str(uuid.uuid4())         # unique ID for this upload

    print(f"\n🐾 Pawprint Upload")
    print(f"   File   : {file_path.name}")
    print(f"   Pet ID : {pet_id}")
    print(f"   Asset  : {asset_id}")
    print()

    # ── Guard: check env vars are set ─────────────────────
    missing = [v for v in ["GCP_PROJECT_ID", "GCS_BUCKET"] if not os.getenv(v)]
    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        print("   Make sure your .env file is filled in and you're in the pawprint/ directory.")
        sys.exit(1)

    try:
        # 1. Validate
        print("Step 1 · Validating file...")
        mime_type  = validate_file(file_path)
        asset_type = infer_asset_type(file_path)

        # 2. Upload to GCS
        print("\nStep 2 · Uploading to GCS...")
        gcs_uri = upload_to_gcs(file_path, pet_id, asset_id, mime_type)

        # 3. Publish Pub/Sub trigger
        print("\nStep 3 · Triggering ingestion pipeline...")
        message_id = publish_to_pubsub(
            pet_id     = pet_id,
            asset_id   = asset_id,
            gcs_uri    = gcs_uri,
            filename   = file_path.name,
            asset_type = asset_type,
            mime_type  = mime_type,
        )

        # ── Summary ───────────────────────────────────────
        print(f"""
✅ Done!

   Asset ID   : {asset_id}
   GCS URI    : {gcs_uri}
   Pub/Sub ID : {message_id}

   Next: the ingestion pipeline will pick this up and run:
   DLP scan → Document AI OCR → Gemini extraction → BigQuery
""")

    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"\n❌ {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
