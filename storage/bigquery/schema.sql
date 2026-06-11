-- =============================================================
-- Pawprint · BigQuery Schema DDL
-- Dataset: pawprint_db (or configured via BQ_DATASET env var)
-- All tables: US multi-region, partitioned + clustered for cost
-- =============================================================


-- -------------------------------------------------------------
-- pets
-- Master record for each pet. One row per pet.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.pets` (
    pet_id          STRING    NOT NULL,   -- UUID, primary key
    owner_id        STRING    NOT NULL,   -- future: multi-user support
    name            STRING    NOT NULL,
    species         STRING    NOT NULL,   -- 'dog' | 'cat' | 'rabbit' ...
    breed           STRING,
    sex             STRING,               -- 'male' | 'female' | 'unknown'
    neutered        BOOL,
    date_of_birth   DATE,
    weight_kg       FLOAT64,              -- latest known weight
    colour          STRING,
    microchip_id    STRING,
    allergies       ARRAY<STRING>,        -- known allergens
    chronic_conditions ARRAY<STRING>,     -- e.g. ['diabetes', 'IBD']
    primary_vet     STRING,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
)
OPTIONS (
    description = 'Master pet registry. One row per pet.'
);


-- -------------------------------------------------------------
-- health_events
-- Every vet visit, diagnosis, procedure, vaccine, or lab result.
-- Partitioned by event_date for cost-efficient time-range queries.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.health_events` (
    event_id        STRING    NOT NULL,   -- UUID
    pet_id          STRING    NOT NULL,
    event_date      DATE      NOT NULL,   -- partition key
    event_type      STRING    NOT NULL,   -- 'vet_visit' | 'vaccine' | 'lab' | 'procedure' | 'diagnosis'
    title           STRING    NOT NULL,   -- short label, e.g. "Annual wellness exam"
    description     STRING,              -- free text extracted from PDF
    provider_name   STRING,              -- vet / clinic name
    diagnoses       ARRAY<STRING>,       -- ICD-10 codes or plain text
    procedures      ARRAY<STRING>,
    vaccines        ARRAY<STRING>,
    lab_results     JSON,                -- flexible KV for any lab panel
    notes           STRING,              -- additional vet notes
    source_doc_id   STRING,              -- FK → media_assets.asset_id (original PDF)
    extraction_confidence FLOAT64,       -- Document AI / Gemini confidence score
    created_at      TIMESTAMP NOT NULL
)
PARTITION BY event_date
CLUSTER BY pet_id, event_type
OPTIONS (
    description     = 'All health events: vet visits, vaccines, labs, procedures.',
    partition_expiration_days = NULL     -- retain indefinitely
);


-- -------------------------------------------------------------
-- medications
-- Active and historical medications per pet.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.medications` (
    medication_id   STRING    NOT NULL,
    pet_id          STRING    NOT NULL,
    name            STRING    NOT NULL,   -- drug name
    dosage          STRING,              -- e.g. "5mg"
    frequency       STRING,              -- e.g. "twice daily"
    route           STRING,              -- 'oral' | 'topical' | 'injection'
    start_date      DATE      NOT NULL,  -- partition key
    end_date        DATE,                -- NULL = currently active
    prescribing_vet STRING,
    indication      STRING,              -- reason / diagnosis it treats
    notes           STRING,
    is_active       BOOL      NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
)
PARTITION BY start_date
CLUSTER BY pet_id
OPTIONS (
    description = 'Medication history. is_active=TRUE for current prescriptions.'
);


-- -------------------------------------------------------------
-- media_assets
-- All uploaded files: PDFs, photos, videos.
-- Embedding vector stored externally in Vertex AI Vector Search.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.media_assets` (
    asset_id        STRING    NOT NULL,
    pet_id          STRING    NOT NULL,
    asset_type      STRING    NOT NULL,  -- 'photo' | 'pdf' | 'video' | 'audio'
    upload_date     DATE      NOT NULL,  -- partition key
    gcs_uri         STRING    NOT NULL,  -- gs://bucket/path/to/file
    filename        STRING    NOT NULL,
    file_size_bytes INT64,
    mime_type       STRING,
    caption         STRING,             -- user-provided or AI-generated
    auto_tags       ARRAY<STRING>,      -- AI-generated labels (e.g. 'park', 'sleeping')
    embedding_id    STRING,             -- ID in Vertex AI Vector Search index
    ocr_text        STRING,             -- extracted text (PDFs only)
    taken_at        TIMESTAMP,          -- EXIF or user-specified timestamp
    location        STRING,             -- optional geo label
    dlp_scanned     BOOL    DEFAULT FALSE,
    dlp_findings    JSON,               -- Cloud DLP output if PII/PHI found
    created_at      TIMESTAMP NOT NULL
)
PARTITION BY upload_date
CLUSTER BY pet_id, asset_type
OPTIONS (
    description = 'All uploaded media. Photos carry multimodal embeddings in Vector Search.'
);


-- -------------------------------------------------------------
-- daily_logs
-- Lightweight structured daily entries: feeding, weight, behaviour.
-- High-volume table — partition by log_date is essential.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.daily_logs` (
    log_id          STRING    NOT NULL,
    pet_id          STRING    NOT NULL,
    log_date        DATE      NOT NULL,  -- partition key
    log_type        STRING    NOT NULL,  -- 'feeding' | 'weight' | 'behaviour' | 'note' | 'symptom'
    value_numeric   FLOAT64,            -- e.g. weight in kg, food in grams
    value_text      STRING,             -- free text for notes / symptoms
    unit            STRING,             -- 'kg' | 'g' | 'cups' etc.
    source          STRING,             -- 'user' | 'voice' | 'auto'
    created_at      TIMESTAMP NOT NULL
)
PARTITION BY log_date
CLUSTER BY pet_id, log_type
OPTIONS (
    description = 'Daily structured logs: feeding, weight, behaviour, symptom notes.'
);


-- -------------------------------------------------------------
-- triage_sessions
-- Audit log of every RAG triage query + response.
-- Enables debugging, accuracy tracking, and future fine-tuning.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${project}.${dataset}.triage_sessions` (
    session_id          STRING    NOT NULL,
    pet_id              STRING    NOT NULL,
    created_at          TIMESTAMP NOT NULL,  -- partition key (by DATE)
    user_query          STRING    NOT NULL,  -- raw symptom description
    retrieved_doc_ids   ARRAY<STRING>,       -- Vector Search result IDs used
    augmented_prompt    STRING,             -- full prompt sent to Gemini
    gemini_response     STRING,             -- raw Gemini output
    urgency_level       STRING,             -- 'emergency' | 'urgent' | 'monitor' | 'routine'
    parsed_causes       ARRAY<STRING>,      -- structured from Gemini response
    recommended_action  STRING,
    model_version       STRING,             -- e.g. 'gemini-1.5-flash-002'
    latency_ms          INT64,
    retrieval_score_avg FLOAT64             -- avg similarity score of retrieved docs
)
PARTITION BY DATE(created_at)
CLUSTER BY pet_id
OPTIONS (
    description = 'RAG triage session audit log. Every query + response recorded.'
);


-- =============================================================
-- Useful views
-- =============================================================

-- Active medications per pet
CREATE OR REPLACE VIEW `${project}.${dataset}.v_active_medications` AS
SELECT
    p.name         AS pet_name,
    p.species,
    m.name         AS medication,
    m.dosage,
    m.frequency,
    m.start_date,
    m.indication
FROM `${project}.${dataset}.medications` m
JOIN `${project}.${dataset}.pets`        p USING (pet_id)
WHERE m.is_active = TRUE;


-- Recent health timeline (last 12 months)
CREATE OR REPLACE VIEW `${project}.${dataset}.v_health_timeline` AS
SELECT
    p.name         AS pet_name,
    he.event_date,
    he.event_type,
    he.title,
    he.provider_name,
    he.diagnoses,
    he.procedures
FROM `${project}.${dataset}.health_events` he
JOIN `${project}.${dataset}.pets`          p USING (pet_id)
WHERE he.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
ORDER BY he.event_date DESC;
