# Executive Brief: Supabase-to-NetSuite Data Pipeline

**Date:** 2026-02-15

---

## The Problem

Your team built a modern application on Supabase. Your finance and operations
teams live in NetSuite. Right now, getting data from one to the other is painful:
every record requires its own API call, NetSuite's rate limits kick in fast, and
the integration stalls under any meaningful volume. Records back up. Data gets
stale. People start exporting CSVs.

This isn't a scaling problem — it's an architecture problem. The current approach
(RESTlet endpoints with OAuth 2.0) was designed for one-at-a-time interactions,
not bulk data movement.

## The Solution

We're building a purpose-built data pipeline that moves data from Supabase to
NetSuite the way NetSuite was designed to receive it: in bulk, reliably, and
continuously.

### What It Does

**Automatic change detection.** When a record changes in Supabase — a new
customer, an updated invoice, a corrected line item — the pipeline detects it
within seconds. No manual exports. No scheduled file drops. No one has to
remember to press a button.

**Bulk delivery that respects NetSuite's limits.** Instead of hammering NetSuite
with thousands of individual API calls, the pipeline groups records into
optimized batches. The same volume of data that currently triggers rate limits
will flow through in a fraction of the time, with no errors.

**Smart ordering.** Customers and vendors load before the invoices and orders
that reference them. The pipeline understands record dependencies and sequences
the data automatically, eliminating the "missing reference" errors that plague
manual imports.

**Self-healing.** If a record fails — a validation error, a temporary NetSuite
outage, a network hiccup — the pipeline retries it automatically. Records that
can't be resolved are quarantined and retried later, with full visibility into
what failed and why. Nothing is silently dropped.

**Zero data loss.** The pipeline tracks exactly where it left off. If it's
restarted, interrupted, or crashes, it picks up right where it stopped. Every
record in Supabase will eventually reach NetSuite.

### What You Can Sync

Any data that lives in Supabase and has a home in NetSuite:

- **Customers and vendors** — keep your CRM and ERP in lockstep
- **Items and inventory** — product catalog changes flow automatically
- **Invoices and sales orders** — transactions appear in NetSuite as they're created
- **Journal entries** — financial data moves without manual re-entry
- **Custom records** — the pipeline is fully configurable for any NetSuite record type, including custom fields

Adding a new data type is a configuration change, not a code change.

## Key Benefits

### For Operations

- **Data is current.** NetSuite reflects Supabase changes in near-real-time,
  not after a nightly batch or a manual export.
- **No more manual imports.** Eliminates CSV exports, copy-paste, and the errors
  that come with them.
- **No more missing references.** Dependency ordering means invoices never fail
  because the customer hasn't been created yet.

### For Engineering

- **No more rate limit firefighting.** Bulk operations reduce API calls by up to
  200x compared to the current approach.
- **Configuration over code.** New record types and field mappings are added via
  YAML, not by writing and deploying new integration code.
- **Full observability.** Structured logging shows exactly what was sent, what
  succeeded, what failed, and why.

### For the Business

- **Faster close cycles.** Financial data arrives in NetSuite continuously instead
  of in delayed batches, accelerating month-end and reporting.
- **Reduced integration risk.** Idempotent operations mean the pipeline can be
  safely retried, restarted, or re-run without creating duplicate records.
- **Lower maintenance cost.** A single, well-tested pipeline replaces fragile
  point-to-point scripts and manual processes.

## How It Compares

|                         | Current (RESTlet)  | This Pipeline                       |
|-------------------------|--------------------|-------------------------------------|
| Records per API call    | 1                  | Up to 200                           |
| Rate limit issues       | Constant           | Eliminated                          |
| Change detection        | Manual / scheduled | Automatic, near-real-time           |
| Failed record handling  | Silent failures    | Automatic retry + dead-letter queue |
| Adding new record types | Write new code     | Edit a config file                  |
| Data loss on restart    | Possible           | Impossible (watermark tracking)     |
| Duplicate records       | Likely on retry    | Impossible (idempotent upserts)     |

## What We Deliver

1. **A running pipeline** that continuously syncs configured Supabase tables to
   NetSuite record types
2. **Three operating modes** — continuous sync, scheduled batch, and bulk backfill
   for initial loads or recovery
3. **A mapping configuration system** that lets you add new data types without
   touching code
4. **A dead-letter queue** with full visibility into failed records and automatic
   retry
5. **Deployment flexibility** — runs as a long-lived process, a container, or a
   scheduled job, on any infrastructure

## What We Need From You

- Supabase database connection credentials
- NetSuite account ID and Token-Based Authentication credentials (consumer key/secret,
  token key/secret)
- A list of which Supabase tables map to which NetSuite record types, and the
  field-level mappings
- Access to a NetSuite sandbox environment for testing
