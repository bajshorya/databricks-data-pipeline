# Medicare Part D Lakehouse

A tested Bronze, Silver and Gold batch pipeline on Databricks Free Edition. It turns CMS's yearly Medicare Part D prescriber files into Delta tables and four dashboard queries.

**Status:** complete and running on Databricks Free Edition, **on synthetic data in the CMS file layout**. `data.cms.gov` blocks downloads from the author's network in India (HTTP 403), so real files couldn't be fetched yet. Loading real data is a data step, not a code change. See `docs/progress.md`.

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

## What the synthetic run proved

Numbers below come from `docs/metrics.json` and describe **synthetic data**, not real Medicare data.

- 172,565 Bronze rows over 2 years and 5 states. Planted bad rows were caught exactly: 294 of 294 in 2022 (0.33% quarantined over both years).
- A forced rerun of a year left every table's counts and money totals identical.
- Cost totals agree to the cent across Silver and every Gold summary.
- A file with 5% bad values stopped the job at the Silver quality gate. Gold was untouched, and a forced reload of the good file restored every table exactly.

## Findings

TODO: measured value (needs real CMS data).

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
