# Product Requirements Document: Supabase-to-NetSuite ETL Pipeline

**Date:** 2026-02-15
**Status:** Draft

---

## 1. Problem Statement

A customer is hitting rate limits using RESTlet OAuth 2.0 endpoints to move data from
Supabase into NetSuite. RESTlets make one API call per record, which is fundamentally
incompatible with bulk data loading. The customer needs both master data (customers,
vendors, items) and transactional records (invoices, sales orders, journal entries)
to flow from Supabase into NetSuite in near-real-time.

## 2. Goal

Get NetSuite to absorb data from Supabase with as little impedance as possible,
replacing the current RESTlet approach with a pipeline that minimizes API calls
and respects NetSuite's rate limits.

## 3. Solution Overview

A generic, config-driven Python ETL pipeline that:

1. **Detects** changes in Supabase via PostgreSQL LISTEN/NOTIFY (with polling fallback)
2. **Accumulates** changes into priority-ordered micro-batches
3. **Loads** batches into NetSuite via SOAP `upsertList` (up to 200 records per call)
4. **Tracks** sync state and failed records for reliability

### Architecture

```
Supabase (Postgres)
    │
    │  Change Detection
    │  LISTEN/NOTIFY (primary) + watermark polling (fallback)
    │
    ▼
Priority Accumulator (in-memory, per record type)
    │
    │  Micro-Batching
    │  Flush every 5s OR every 200 records (whichever first)
    │
    ▼
NetSuite SOAP upsertList
    │  Up to 200 records/call, TBA auth, concurrency-limited
    │
    ▼
State Store (sync watermarks, dead-letter queue)
```

### Why This Architecture

| Decision                              | Rationale                                               |
|---------------------------------------|---------------------------------------------------------|
| SOAP `upsertList` over RESTlets       | ~200x fewer API calls (200 records/call vs 1)           |
| External ID-based upsert              | Idempotent — safe to retry without deduplication        |
| Token-Based Auth (TBA) over OAuth 2.0 | No token refresh storms; one auth setup                 |
| LISTEN/NOTIFY + polling fallback      | Near-real-time with reliability guarantee               |
| Priority-ordered flushing             | Master data loads before transactions that reference it |
| 200-record batches (not 1,000)        | Smaller blast radius; avoids timeout risk               |

## 4. Requirements

### 4.1 Functional Requirements

**FR-1: Generic Mapping**
The pipeline must support arbitrary Supabase table-to-NetSuite record type mappings
via YAML configuration, including standard fields, custom fields (`custbody_*`,
`custentity_*`, `custcol_*`), RecordRef cross-references, and sublists.

**FR-2: Near-Real-Time Change Detection**
Changes in Supabase must be detected within seconds under normal operation.
A polling fallback must catch any missed changes within 60 seconds.

**FR-3: Dependency-Ordered Loading**
Master data records (customers, vendors, items) must be loaded before transactional
records (invoices, sales orders) that reference them. Priority is declared per
record type in the mapping configuration.

**FR-4: Idempotent Upserts**
All writes to NetSuite must use `upsertList` with deterministic external IDs
derived from Supabase primary keys. Re-processing the same record must be safe.

**FR-5: Error Handling**
- Partial batch failures must retry only the failed records (up to 3 attempts).
- Rate limit errors must trigger exponential backoff.
- Records that exhaust retries must be routed to a dead-letter queue.
- A reconciliation loop must periodically retry dead-letter records.

**FR-6: State Persistence**
Sync watermarks (per table) must survive process restarts. On restart, the pipeline
must resume from the last confirmed watermark with no data loss.

**FR-7: Multiple Run Modes**
- `run` — continuous near-real-time sync (long-running process)
- `sync-once` — single polling cycle then exit (for cron)
- `backfill` — full or partial table scan for initial load or recovery

### 4.2 Non-Functional Requirements

**NFR-1: Rate Limit Compliance**
The pipeline must never exceed NetSuite's concurrency limit (configurable,
default 5 concurrent SOAP calls). Must handle `ExceededRequestLimitFault`
gracefully with exponential backoff.

**NFR-2: Observability**
Structured JSON logging with per-batch metrics: records sent, records failed,
NetSuite response time, retry counts.

**NFR-3: Configurability**
All tuning parameters (batch size, flush interval, concurrency limit, retry
counts, polling interval) must be configurable via YAML and/or environment
variables.

**NFR-4: Testability (TDD)**
All modules must be independently unit-testable without I/O. Integration tests
must run against a local Postgres (via Docker) and optionally a NetSuite sandbox.

## 5. Tech Stack

| Component         | Technology                       | Purpose                                                          |
|-------------------|----------------------------------|------------------------------------------------------------------|
| Language          | Python 3.11+                     | Best ecosystem for NetSuite SOAP                                 |
| SOAP Client       | `zeep`                           | Mature Python SOAP library; call `upsertList` via SuiteTalk WSDL |
| Postgres Client   | `asyncpg`                        | Async LISTEN/NOTIFY + queries                                    |
| Config Validation | `pydantic` / `pydantic-settings` | Type-safe settings from env vars + YAML                          |
| Mapping Config    | `pyyaml`                         | Declarative field mappings                                       |
| CLI               | `click`                          | Command-line interface                                           |
| Logging           | `structlog`                      | Structured JSON logging                                          |
| Testing           | `pytest` / `pytest-asyncio`      | TDD; unit + integration tests                                    |

## 6. Data Flow

### 6.1 Change Detection

**Primary: LISTEN/NOTIFY**

Postgres trigger functions fire `pg_notify('etl_changes', payload)` on
INSERT/UPDATE for each tracked table. The payload contains the table name,
operation, primary key, and `updated_at` timestamp. The Python process listens
via `asyncpg`.

**Fallback: Watermark Polling**

Every 60 seconds, query each tracked table:
```sql
SELECT * FROM {table} WHERE updated_at > {last_watermark} ORDER BY updated_at LIMIT 500
```
Catches records missed during NOTIFY connection drops or direct SQL modifications
that bypass triggers.

### 6.2 Micro-Batching

Changes accumulate in a priority-keyed in-memory queue. Flush triggers:
- **Time-based:** Every 5 seconds (configurable)
- **Size-based:** When 200 records accumulate for any record type
- Whichever threshold hits first

### 6.3 Priority-Ordered Flush

Within each flush cycle, records are sent in priority order:
1. **Priority 1** — Entities: Customers, Vendors
2. **Priority 2** — Items
3. **Priority 3** — Transactions: Invoices, Sales Orders, Journal Entries

Each priority tier is flushed sequentially. Within a tier, batches of up to
200 records are sent via `upsertList`. Calls within the same tier may run
concurrently up to the concurrency limit.

### 6.4 External ID Convention

Format: `{prefix}{supabase_pk}`

Example: Supabase customer with `id = a1b2c3d4` → NetSuite external ID `SUP_CUST_a1b2c3d4`

Cross-references in transactions (e.g., an invoice's `entity` field referencing a
customer) use the same prefix convention, so a RecordRef is constructed from the
foreign key without querying NetSuite for internal IDs.

## 7. Configuration

### 7.1 Runtime Settings (`config/settings.yaml`)

```yaml
supabase:
  database_url: "${SUPABASE_DATABASE_URL}"

netsuite:
  account: "${NETSUITE_ACCOUNT}"
  consumer_key: "${NETSUITE_CONSUMER_KEY}"
  consumer_secret: "${NETSUITE_CONSUMER_SECRET}"
  token_key: "${NETSUITE_TOKEN_KEY}"
  token_secret: "${NETSUITE_TOKEN_SECRET}"
  max_concurrency: 5
  soap_timeout: 120

pipeline:
  flush_interval_seconds: 5
  max_batch_size: 200
  poll_interval_seconds: 60
  max_retries: 5
  retry_base_delay_seconds: 2
  dead_letter_retry_interval_seconds: 900

state:
  backend: "supabase"   # or "sqlite"
```

### 7.2 Mapping Configuration (`config/mappings.yaml`)

```yaml
record_types:
  customer:
    supabase_table: "customers"
    netsuite_type: "Customer"
    netsuite_namespace: "urn:relationships_2017_1.lists.webservices.netsuite.com"
    external_id_source: "id"
    external_id_prefix: "SUP_CUST_"
    priority: 1
    fields:
      entityId: "customer_code"
      companyName: "company_name"
      email: "email_address"
    custom_fields:
      custentity_source_system: "'supabase'"    # literal value
      custentity_supabase_id: "id"              # mapped from column
    sublists:
      addressbookList:
        source_table: "customer_addresses"
        foreign_key: "customer_id"
        fields:
          addr1: "street_line_1"
          city: "city"
          state: "state_code"
          zip: "postal_code"

  invoice:
    supabase_table: "invoices"
    netsuite_type: "Invoice"
    netsuite_namespace: "urn:sales_2017_1.transactions.webservices.netsuite.com"
    external_id_source: "id"
    external_id_prefix: "SUP_INV_"
    priority: 3
    depends_on: ["customer", "item"]
    fields:
      tranId: "invoice_number"
      entity:
        type: "RecordRef"
        target_type: "customer"
        external_id_source: "customer_id"
        external_id_prefix: "SUP_CUST_"
    sublists:
      itemList:
        source_table: "invoice_lines"
        foreign_key: "invoice_id"
        fields:
          item:
            type: "RecordRef"
            target_type: "item"
            external_id_source: "item_id"
            external_id_prefix: "SUP_ITEM_"
          quantity: "qty"
          rate: "unit_price"
```

## 8. Error Handling

| Failure Type                | Action                                                    |
|-----------------------------|-----------------------------------------------------------|
| `ExceededRequestLimitFault` | Exponential backoff (2s, 4s, 8s, 16s, 32s), max 5 retries |
| Network timeout             | Retry once (upsert is idempotent via external ID)         |
| Per-record validation error | Do NOT retry; route to dead-letter queue                  |
| `InvalidSessionFault`       | Re-authenticate, then retry                               |
| Partial batch failure       | Retry only failed records (up to 3 times)                 |
| Dead-letter records         | Reconciliation loop retries every 15 minutes              |

### Dead-Letter Schema

```sql
CREATE TABLE sync_failures (
    id SERIAL PRIMARY KEY,
    record_type TEXT NOT NULL,
    external_id TEXT NOT NULL,
    supabase_pk TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    payload JSONB,
    retry_count INTEGER DEFAULT 0,
    first_failed_at TIMESTAMPTZ DEFAULT NOW(),
    last_failed_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);
```

## 9. State Management

### Sync State Schema

```sql
CREATE TABLE sync_state (
    table_name TEXT PRIMARY KEY,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01',
    last_synced_pk TEXT,
    records_synced BIGINT DEFAULT 0,
    last_error TEXT,
    last_error_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

Watermarks advance only after confirmed success. On process restart, the polling
fallback queries from the last watermark — no data loss is possible since the
source of truth (Supabase) is immutable.

State backend is configurable: Supabase table (recommended for production) or
local SQLite (for development/testing).

## 10. Project Structure

```
netsuite_etl/
├── pyproject.toml
├── PRD.md
├── config/
│   ├── settings.yaml
│   └── mappings.yaml
├── src/netsuite_etl/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── pipeline.py
│   ├── errors.py
│   ├── config/
│   │   ├── settings.py
│   │   └── mappings.py
│   ├── detection/
│   │   ├── listener.py
│   │   ├── poller.py
│   │   └── detector.py
│   ├── batching/
│   │   ├── accumulator.py
│   │   └── flush_manager.py
│   ├── netsuite/
│   │   ├── auth.py
│   │   ├── client.py
│   │   ├── record_builder.py
│   │   └── response_parser.py
│   ├── mapping/
│   │   ├── engine.py
│   │   ├── external_id.py
│   │   └── transforms.py
│   └── state/
│       ├── backend.py
│       ├── manager.py
│       └── dead_letter.py
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_external_id.py
│   │   ├── test_mapping_engine.py
│   │   ├── test_record_builder.py
│   │   ├── test_response_parser.py
│   │   ├── test_accumulator.py
│   │   ├── test_flush_manager.py
│   │   ├── test_auth.py
│   │   └── test_state_manager.py
│   ├── integration/
│   │   ├── test_netsuite_client.py
│   │   └── test_pipeline_e2e.py
│   └── fixtures/
│       ├── sample_mappings.yaml
│       └── sample_responses.xml
├── migrations/
│   ├── 001_create_sync_state.sql
│   ├── 002_create_sync_failures.sql
│   └── 003_create_notify_triggers.sql
└── docker-compose.yaml
```

## 11. CLI Interface

```bash
# Near-real-time continuous sync
netsuite-etl run --config config/settings.yaml --mappings config/mappings.yaml

# One-shot batch (for cron)
netsuite-etl sync-once --config config/settings.yaml --mappings config/mappings.yaml

# Initial load / recovery
netsuite-etl backfill --tables customers,items --since 2024-01-01
```

## 12. Implementation Phases (TDD)

### Phase 1: Foundation
1. Project scaffolding (`pyproject.toml`, directory structure)
2. Config: `settings.py` (Pydantic), `mappings.py` (YAML loader)
3. `external_id.py` — ID generation
4. `mapping/engine.py` — row-to-dict mapping
5. `errors.py` — exception types

### Phase 2: NetSuite Integration
6. `netsuite/auth.py` — TBA TokenPassport
7. `netsuite/client.py` — zeep SOAP wrapper
8. `netsuite/record_builder.py` — dict to zeep objects
9. `netsuite/response_parser.py` — parse responses

### Phase 3: Change Detection
10. `detection/listener.py` — LISTEN/NOTIFY
11. `detection/poller.py` — watermark polling
12. `detection/detector.py` — orchestrator

### Phase 4: Batching & State
13. `batching/accumulator.py` — priority queue
14. `batching/flush_manager.py` — flush + retry
15. `state/` — backend, manager, dead-letter

### Phase 5: Assembly
16. `pipeline.py` — async orchestrator
17. `cli.py` + `__main__.py`
18. SQL migrations
19. Integration tests
20. Docker / deployment config

## 13. Verification & Acceptance Criteria

1. **Unit tests pass:** `pytest tests/unit/` — all green, no I/O required
2. **Change detection works:** Insert a row in local Postgres, verify
   LISTEN/NOTIFY delivers the event within 1 second
3. **Polling fallback works:** Kill NOTIFY listener, insert a row, verify
   poller detects it within 60 seconds
4. **NetSuite integration works:** Upsert a batch of test records to
   NetSuite sandbox, verify they appear via saved search
5. **Dependency ordering works:** Sync a customer and an invoice that
   references it; verify the customer is created first
6. **Error handling works:** Send a batch with an invalid record; verify
   valid records succeed, invalid record lands in dead-letter queue
7. **Rate limit handling works:** Run a backfill of 1,000+ records;
   verify no unhandled 429 errors and backoff engages if limits are hit
8. **Idempotency works:** Run the same sync twice; verify no duplicate
   records in NetSuite
