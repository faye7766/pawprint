# Pawprint 🐾

**GCP-native RAG platform for pet health triage and memory search.**

A production-grade AI Data Engineering portfolio project demonstrating end-to-end design,
build, and deployment of a retrieval-augmented generation (RAG) system on Google Cloud Platform.

---

## What it does

| Feature | Description |
|---|---|
| **Health Triage Agent** | Upload a photo or describe symptoms → RAG pipeline retrieves relevant vet knowledge + pet's own medical history → Gemini generates a structured triage response (urgency, likely causes, next steps) |
| **Semantic Photo Search** | Natural language search over a pet's photo archive ("find photos of Mochi at the park last summer") powered by multimodal embeddings |
| **Medical Record Ingestion** | Upload vet visit PDFs → Document AI OCR → Gemini structured extraction → BigQuery storage |
| **Health Timeline** | Longitudinal view of all health events, medications, and daily logs per pet |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                     │
│  PDF/Photos → GCS │ Voice/Text → Pub/Sub │ Cloud DLP   │
└──────────────────────────┬──────────────────────────────┘
                           │ Cloud Composer (Airflow) orchestrates
┌──────────────────────────▼──────────────────────────────┐
│                    PROCESSING LAYER                     │
│  Document AI (OCR) │ Gemini 1.5 Flash (extraction)     │
│  Vertex AI Embeddings │ Cloud Functions (triggers)      │
│  ─────────────────────────────────────────────────────  │
│  RAG Chain: Vector Search → context augmentation →      │
│             Gemini generation → triage response         │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                      STORAGE LAYER                      │
│  BigQuery (structured health records + daily logs)      │
│  Vertex AI Vector Search (embeddings index)             │
│  Cloud Storage (raw files, processed PDFs, photos)      │
│  Veterinary Knowledge Base (chunked + embedded)         │
│  Dataform (SQL transforms) │ Data Catalog (lineage)     │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│                      SERVING LAYER                      │
│  FastAPI (REST endpoints) │ Streamlit (UI)              │
│  Cloud Run (containerised, auto-scale to zero)          │
└─────────────────────────────────────────────────────────┘
              IAM · VPC Service Controls · Audit Logging
```

Full architecture diagram: [`docs/architecture.md`](docs/architecture.md)

---

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | Cloud Composer (Apache Airflow 2.x) |
| Ingestion | GCS, Pub/Sub, Cloud Functions |
| AI / ML | Vertex AI (Gemini 1.5 Flash, text-embedding-004, multimodalembedding), Document AI |
| RAG framework | LangChain + Vertex AI Vector Search |
| Structured storage | BigQuery (partitioned + clustered tables) |
| Transforms | Dataform (SQL-based, version-controlled) |
| Serving | FastAPI + Cloud Run + Streamlit |
| Governance | Cloud DLP, Data Catalog, IAM, Cloud Monitoring |
| IaC | Terraform (planned) |
| CI/CD | GitHub Actions → Cloud Build |
| Language | Python 3.11 |

---

## Repository structure

```
pawprint/
├── ingestion/
│   ├── gcs/              # GCS upload handlers, file validation
│   └── pubsub/           # Pub/Sub publisher / subscriber
├── processing/
│   ├── document_ai/      # Vet record OCR + entity extraction
│   ├── gemini/           # Gemini structured extraction prompts
│   ├── embeddings/       # Vertex AI embedding generation
│   └── rag/              # RAG chain: retrieval + generation
├── storage/
│   ├── bigquery/         # Schema DDL, table creation, query helpers
│   └── vector_search/    # Index creation, upsert, query
├── orchestration/
│   └── dags/             # Cloud Composer Airflow DAGs
├── serving/
│   ├── api/              # FastAPI app + route definitions
│   └── app/              # Streamlit UI
├── tests/                # Unit + integration tests
├── scripts/              # Setup, seed data, local dev helpers
├── notebooks/            # Exploration + prototyping
├── docs/                 # Architecture docs, ADRs, schema diagrams
├── .env.example          # Required environment variables
├── requirements.txt
├── Dockerfile
└── README.md
```

---

## Data model (BigQuery)

Core tables — see [`storage/bigquery/schema.sql`](storage/bigquery/schema.sql) for full DDL.

| Table | Partition | Cluster | Description |
|---|---|---|---|
| `pets` | — | `owner_id` | Master pet record (breed, DOB, allergies) |
| `health_events` | `event_date` | `pet_id, event_type` | Vet visits, diagnoses, procedures |
| `medications` | `start_date` | `pet_id` | Active + historical medications |
| `media_assets` | `upload_date` | `pet_id, asset_type` | Photos, videos, documents |
| `daily_logs` | `log_date` | `pet_id` | Feeding, weight, behaviour notes |
| `triage_sessions` | `created_at` | `pet_id` | RAG query + response audit log |

---

## Roadmap

### Phase 1 — Foundation (Weeks 1–4) ✅ in progress
- [x] Architecture design + repo structure
- [x] BigQuery schema DDL
- [ ] GCS ingestion handler
- [ ] Pub/Sub event trigger
- [ ] Document AI OCR pipeline (vet PDF → structured JSON)
- [ ] Gemini structured extraction (JSON → BigQuery)
- [ ] Cloud Composer DAG (orchestrates full ingestion flow)

### Phase 2 — RAG core (Weeks 5–8)
- [ ] Veterinary knowledge base: scrape + chunk + embed
- [ ] Vertex AI Vector Search index creation + upsert
- [ ] RAG chain: symptom query → retrieve → augment → generate
- [ ] Triage response schema (urgency level, causes, next steps)
- [ ] FastAPI endpoint: `/triage` + `/search`

### Phase 3 — Serving + polish (Weeks 9–12)
- [ ] Streamlit UI: chat interface + photo search + timeline
- [ ] Cloud Run deployment (Dockerfile + CI/CD)
- [ ] Dataform transforms + data quality checks
- [ ] Cloud DLP integration (PII scan on ingestion)
- [ ] Cloud Monitoring dashboards + alerting
- [ ] Data Catalog tagging + lineage

---

## Setup

### Prerequisites
- GCP project with billing enabled
- APIs enabled: Vertex AI, Document AI, BigQuery, Pub/Sub, Cloud Composer, Cloud Run, Cloud DLP
- `gcloud` CLI authenticated
- Python 3.11+

### Local dev

```bash
git clone https://github.com/faye7766/pawprint.git
cd pawprint
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your GCP project values
```

### Environment variables

See [`.env.example`](.env.example) for all required variables.
Key variables: `GCP_PROJECT_ID`, `GCP_REGION`, `BQ_DATASET`, `VECTOR_SEARCH_INDEX_ID`.

---

## Design decisions

See [`docs/adr/`](docs/adr/) for Architecture Decision Records covering:
- Why Vertex AI Vector Search over alternatives (Pinecone, Weaviate)
- Why Gemini 1.5 Flash over Pro for extraction tasks
- Why Dataform over dbt for SQL transforms in this GCP-native stack
- Why Cloud Run over GKE for serving at this scale

---

## Disclaimer

Pawprint is a portfolio / research project. Triage outputs are **informational only**
and do not constitute veterinary advice. Always consult a licensed veterinarian for
health decisions.

---

*Built by Yifei (Faye) Wang · Senior Data Engineer → AI Data Engineer*
*Stack: GCP · Vertex AI · BigQuery · LangChain · Python*
