# ADR-001: Vector database — Vertex AI Vector Search vs alternatives

**Date:** 2026-06  
**Status:** Accepted  
**Author:** Yifei (Faye) Wang

---

## Context

The RAG pipeline requires a vector database to store and retrieve embeddings for:
- Vet record health events (~thousands of documents per user)
- Veterinary knowledge base (~50k–100k chunks from public vet literature)
- Pet photo multimodal embeddings (future phase)

Options evaluated: Vertex AI Vector Search, Pinecone, Weaviate, pgvector (Cloud SQL).

## Decision

**Vertex AI Vector Search** (formerly Matching Engine).

## Reasons

1. **GCP-native integration**: No cross-network calls, no extra auth layer. Direct SDK calls from Cloud Functions and Cloud Run within the same VPC. Latency advantage for a latency-sensitive triage use case.

2. **IAM-based access control**: Consistent with the rest of the stack. No separate API key management plane.

3. **Resume / portfolio positioning**: The project is designed to demonstrate GCP expertise. Using a non-GCP vector DB weakens the "fully GCP-native" story.

4. **Scale ceiling**: Vertex AI Vector Search handles billions of vectors. Overkill for this project but demonstrates production-grade thinking.

5. **Managed ops**: No cluster to manage. Index updates are async; no downtime for upserts.

## Trade-offs accepted

- Higher cold-start cost for small indexes (Vector Search has a minimum deployment cost even for small indexes — ~$0.60/hr for a deployed endpoint).
- Less community tooling than Pinecone.
- Weaviate's GraphQL API and built-in hybrid search (BM25 + vector) are more flexible; accepted as out-of-scope.

## Mitigation

For local development and cost control during early phases, use an in-memory FAISS index (via LangChain's FAISS wrapper) and swap to Vector Search for deployment. The abstraction layer in `storage/vector_search/` makes this swap transparent.
