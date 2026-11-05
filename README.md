# Medicare Part D Lakehouse

A tested Bronze, Silver and Gold batch pipeline on Databricks Free Edition. It turns CMS's yearly Medicare Part D prescriber files into Delta tables and four dashboard queries.

**Status:** the pipeline code, tests and CI are complete and pass locally. It has **not yet run on Databricks or on real CMS data** (see `docs/progress.md`). Findings and screenshots below are placeholders until it does.

## Architecture

```text
CMS CSV (streamed, filtered to chosen states, gzipped, hashed)   scripts/fetch_cms.py
        -> Volume (landing)                                       scripts/upload_landing.py
        -> Bronze   all-string, as received, + file hash          src/partd/bronze.py
        -> Silver   typed, cleaned, bad rows quarantined          src/partd/silver.py
        -> Gold     SQL aggregates, YoY, coverage                 sql/gold/*.sql
        checks (dq_results) between layers                        src/partd/quality.py
        5 serverless job tasks, deployed with a bundle            databricks.yml, resources/job.yml
```

## Findings

TODO: measured value (needs a real run).

## Data limits

- The detail file excludes rows with fewer than 11 claims, so totals undercount. `reconciliation_year` measures coverage against CMS state totals.
- A blank number means suppressed (1 to 10). It is stored as NULL, never 0.
- Beneficiaries are never summed across rows.
- Costs exclude manufacturer rebates. They aren't Medicare Trust Fund payments.
- The data doesn't indicate quality of care. CMS's opioid drug list can change yearly.
- Results are published by state, specialty and drug only. Prescriber names are never loaded into Silver or Gold.
- Scope: a subset of states and two years (2023, 2024), chosen to fit a small laptop and Free Edition quotas.

## Run it

```bash
python3.12 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest -q && .venv/bin/ruff check .
.venv/bin/python scripts/fetch_cms.py --dataset geo_drug --year 2024 --states RI --head-only
databricks auth login --host https://YOUR-WORKSPACE-URL
# set the host in databricks.yml, then:
databricks bundle deploy -t dev
.venv/bin/python scripts/upload_landing.py
databricks bundle run partd_pipeline -t dev --params data_year=2024
```

## Design decisions

See `docs/decisions.md`. At 100x the data: partition by state, use Auto Loader, and run on a cluster.
