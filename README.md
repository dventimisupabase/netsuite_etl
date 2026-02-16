# netsuite_etl

A config-driven Python pipeline that moves data from Supabase into NetSuite via SOAP `upsertList`.

## Why

NetSuite's API rate limits make single-record integrations (RESTlets, REST API) impractical for bulk data. This pipeline batches up to 200 records per API call using SOAP `upsertList` with idempotent external IDs, reducing API calls by orders of magnitude.

## How It Works

1. **Detect** — listens for changes in Supabase via Postgres LISTEN/NOTIFY (with polling fallback)
2. **Batch** — accumulates changes into priority-ordered micro-batches (master data before transactions)
3. **Load** — sends batches to NetSuite via SOAP `upsertList` with Token-Based Authentication
4. **Track** — persists sync watermarks and routes failed records to a dead-letter queue

## Docs

- [PRD](PRD.md) — technical requirements and architecture
- [Executive Brief](EXECUTIVE_BRIEF.md) — features and benefits overview
