# Databricks Feedback Loop

Feedback API service and ingestion pipeline for model feedback. Deployed as a Databricks Asset Bundle (DAB) and exposed via a Databricks App.

## Quickstart (local)
1) Install dependencies
   - `python -m pip install -r requirements.txt -r requirements-dev.txt`
2) Run the API:
   - `uvicorn app.app:app --reload`

## Deploy (Databricks)
1) Configure Databricks credentials (for CLI):
   - `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`
2) Deploy the bundle:
   - `databricks bundle deploy`
3) Provision the feedback table (DDL job):
   - `databricks bundle run feedback-table-ddl`

## Project structure
- `app/` - Databricks app entrypoint and API implementation
- `resources/` - DAB resources (apps + jobs)
- `sql/` - SQL DDL used by the table provisioning job
- `scripts/` - Optional operational tooling (i.e. retention purge)
- `tests/` - Unit tests
- `.github/workflows/` - CI/CD pipeline for bundle validate/deploy