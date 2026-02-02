# Databricks Feedback Loop

Feedback API service and ingestion pipeline for model feedback. Deployed as a Databricks Asset Bundle (DAB) and exposed via a Databricks App.

## Prerequisites
- Python 3.10+ (recommended) and `pip`
- Databricks CLI v0.200+ (or current) configured for your workspace
- Access to a Databricks SQL Warehouse ID for ingestion
- Permissions to create and run Databricks Jobs and Apps

## Local development

### 1) Create and activate a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies
```bash
python -m pip install -r requirements.txt -r requirements-dev.txt
```

### 3) Configure environment variables
The API requires at minimum:
- `FEEDBACK_TABLE`: fully qualified Delta table (catalog.schema.table)
- `DATABRICKS_WAREHOUSE_ID`: SQL Warehouse ID used for inserts

Optional configuration:
- `TRACE_TABLE`: trace table for linking (catalog.schema.table)
- `RATE_LIMIT_WINDOW_SECONDS`, `RATE_LIMIT_PER_USER`, `RATE_LIMIT_PER_TOKEN`, `RATE_LIMIT_PER_IP`
- `LOG_LEVEL`
- `SQL_WAIT_TIMEOUT`, `SQL_EXEC_RETRIES`, `SQL_EXEC_BACKOFF_SECONDS`
- `TRACE_ID_POLICY`, `TRACE_ID_REGEX`, `TRACE_ID_MAX_AGE_SECONDS`
- `TRACKING_ID_POLICY`, `TRACKING_ID_REGEX`, `TRACKING_ID_UNIQUENESS_SECONDS`

Example (bash):
```bash
export FEEDBACK_TABLE="llm_sandbox.voice_to_soap.feedback_ingest"
export DATABRICKS_WAREHOUSE_ID="<warehouse-id>"
export TRACE_TABLE="llm_sandbox.inference_tables.trace_logs_2024551179173857"
```

### 4) Run the API locally
```bash
uvicorn app.app:app --reload
```

## API usage
POST `/feedback/submit` expects a JSON payload that matches `FeedbackPayload` in [app/feedback_api/models.py](app/feedback_api/models.py).

Required fields:
- `schema_version` (e.g., `v1`)
- `tracking_id`
- `pims`
- `timestamp` (UTC ISO-8601, e.g. `2026-01-01T00:00:00Z`)

Example request:
```json
{
  "schema_version": "v1",
  "tracking_id": "track-123",
  "trace_id": "trace-abc",
  "pims": "ezyvet",
  "timestamp": "2026-02-02T12:00:00Z",
  "feedback_boolean": true,
  "feedback_score_1": 4,
  "feedback_comment_1": "Helpful"
}
```

Responses:
- `200` accepted with `feedback_id`
- `400` validation or policy failure
- `409` duplicate payload
- `429` rate limiting
- `500` ingestion or policy check failure

## Databricks Asset Bundle (DAB)

### Configure bundle variables
Bundle variables are defined in [databricks.yml](databricks.yml) and used in:
- App resource: [resources/apps/feedback_api.yml](resources/apps/feedback_api.yml)
- Jobs: [resources/jobs/feedback_table_ddl.yml](resources/jobs/feedback_table_ddl.yml) and [resources/jobs/link_feedback_stream.yml](resources/jobs/link_feedback_stream.yml)

Key variables:
- `warehouse_id`, `feedback_table`, `trace_table`
- `trace_index_table`, `link_stream_checkpoint`
- `link_stream_trigger_seconds`, `link_stream_watermark_minutes`
- policy and rate limit variables

Override variables per target using `databricks bundle deploy -t <target> --var key=value` or in the target section of [databricks.yml](databricks.yml).

### Deploy the bundle
```bash
databricks bundle deploy -t dev
```

### Provision the feedback table
```bash
databricks bundle run feedback-table-ddl -t dev
```
The table schema is defined in [sql/feedback_table.sql](sql/feedback_table.sql).

### Deploy and run the linking stream job
```bash
databricks bundle run feedback-linking-stream -t dev
```
This job runs [scripts/stream_linking.py](scripts/stream_linking.py) to continuously link feedback rows to trace data.

### Databricks App deployment
The app is deployed via DAB using the resource in [resources/apps/feedback_api.yml](resources/apps/feedback_api.yml).
It references the source at `app/` and injects environment variables for the API runtime.

## How linking works
Linking follows this order:
1) If `trace_id` is provided and exists in `TRACE_TABLE`, link via `trace_id_match`.
2) Otherwise, look up matching traces by `tracking_id` and link to the most recent `request_time`.
   - If exactly one match: `tracking_id_exact_match`.
   - If multiple: `tracking_id_recent_match`.
3) If no link is possible: `no_match`.

The API always stores the payload; link mode is recorded on write.

## Testing and linting
```bash
pytest
python -m pylint app scripts tests
```

## Troubleshooting
Common issues and fixes:
- **Missing required env vars**: set `FEEDBACK_TABLE` and `DATABRICKS_WAREHOUSE_ID`.
- **SQL permissions**: ensure the SQL warehouse has permissions to read/write the catalog and schema.
- **App deployment**: verify app permissions in [resources/apps/feedback_api.yml](resources/apps/feedback_api.yml).
- **Stream job failures**: verify `trace_table`, `trace_index_table`, and checkpoint path are accessible.

## Project structure
- `app/` - Databricks app entrypoint and API implementation
- `resources/` - DAB resources (apps + jobs)
- `sql/` - SQL DDL used by the table provisioning job
- `scripts/` - Operational tooling (e.g. retention purge, stream linking)
- `tests/` - Unit tests
- `.github/workflows/` - CI/CD pipeline for bundle validate/deploy