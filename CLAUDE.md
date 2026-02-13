# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

This is a **learning project**. The goal is to migrate an existing DuckDB-based data pipeline to dbt, using the process as a hands-on dbt tutorial. The user has limited dbt experience. Claude's role is to **tutor and teach**, not just translate SQL. Explain dbt concepts as they arise, ask the user questions to check understanding, and let them drive decisions.

## Source Pipeline Being Migrated

Blog post: https://rmoff.net/2025/03/20/building-a-data-pipeline-with-duckdb/
SQL files: https://gist.github.com/rmoff/461fd169843063fc1b9b3113759ff5b6

### Data Source

UK Environment Agency flood and river level data from the real-time data API: https://environment.data.gov.uk/flood-monitoring/doc/reference

Three entities are ingested from REST API endpoints:
- **stations** - monitoring stations around the UK
- **measures** - what each station measures (rainfall, river level, etc.)
- **readings** - actual measurement values (the fact data)

Historical readings are also available as daily CSV archives at:
`https://environment.data.gov.uk/flood-monitoring/archive/readings-YYYY-MM-DD.csv`

### Data Model

```
readings (fact) --> measures (dimension) --> stations (dimension)
```

- `readings.measure` -> `measures.notation` (FK)
- `measures.station` -> `stations.notation` (FK)
- Note: `stationReference` is NOT the FK to stations (0.4% mismatch with `station`)

### Key Transformations in the Original Pipeline

1. **Staging**: Raw JSON from API, unnested from `items` array
2. **URL prefix stripping**: Foreign keys arrive as full URLs (e.g., `http://environment.data.gov.uk/flood-monitoring/id/measures/...`) and need the prefix removed to get the usable key
3. **Dimensions** (SCD Type 1): Full rebuild each run via `CREATE OR REPLACE`. Measures excludes `@id` and `latestReading` columns. Stations excludes `measures` column
4. **Fact table** (incremental): `INSERT OR IGNORE` to append new readings, deduplicated on PK `(dateTime, measure)`
5. **Enriched table**: Denormalized join of readings + measures + stations, also incrementally loaded. Columns prefixed with `r_`, `m_`, `s_`

### Data Quirks

- Historical CSV data contains pipe-delimited values (e.g., `0.770|0.688`) in ~0.01% of rows - original pipeline takes the first value
- The `value` column in CSV archives is varchar, not double
- Some stations lag in reporting, producing apparent duplicates when polling
- Several API fields return JSON arrays instead of scalars for some records (e.g., `dateOpened`, `easting`, `northing`, `RLOIid`, `value`). We take the first element using `->>0` or `CASE WHEN json_type() = 'ARRAY'` depending on context

## Design Decisions

Agreed approach for the dbt migration, in phases:

1. **Start simple**: staging models, dimension models (full rebuild), incremental fact model, dbt tests
2. **Iterate on SCD**: introduce dbt snapshots on dimensions to preserve history and avoid orphaned facts
3. **Backfill**: separate handling for historical data from the CSV archive (https://environment.data.gov.uk/flood-monitoring/archive)
4. **Optional enrichment**: dim_date table and daily summary mart as learning exercises
5. **Orchestration**: discuss how to schedule/orchestrate dbt runs (replacing the cron job from the original pipeline)

Key principles from community feedback on the original blog:
- Dimensions should not orphan facts on rebuild -- address via SCD/snapshots in phase 2
- Backfill should be a separate concern from incremental loads
- Logging and data quality checks are important -- dbt tests cover this naturally
- Keep the fact table append-only; dedup in the pipeline not via upsert
- A denormalized "one big table" is acceptable for one fact table but consider star schema patterns as a learning exercise

## Environment

- Python venv at `.venv/` (Python 3.13)
- Target database engine: DuckDB (via dbt-duckdb adapter)
- Platform: macOS

## Common Commands

```bash
source .venv/bin/activate       # activate venv (dbt installed here via uv)
dbt debug                       # verify connection to DuckDB
dbt run                         # build all models
dbt run --select staging        # build all staging models
dbt run --select dim_measures   # build a single model
dbt run --select +dim_measures  # build a model and all its upstream dependencies
```

- DuckDB database: `/Users/rmoff/work/env-agency-dev.duckdb`
- dbt profile: `~/.dbt/profiles.yml`
- All dbt commands should be run from the repo root (`/Users/rmoff/git/env-agency-dbt`)

## Project Structure

```
macros/
  strip_api_url.sql       -- reusable macro to strip API URL prefixes from foreign keys
  load_raw_data.sql       -- run-operation macro to load raw tables from API (simulates ingestion)
  test_max_pct_failing.sql -- custom generic test for threshold-based aggregate checks
models/
  staging/                -- raw data from API, materialized as tables
    stg_stations.sql
    stg_measures.sql
    stg_readings.sql
    sources.yml           -- declares raw tables as dbt sources (with freshness config)
  marts/                  -- transformed business-ready models
    dim_measures.sql        -- table, reads from snap_measures (current records only)
    dim_stations.sql        -- table, reads from snap_stations (current records only)
    dim_date.sql            -- table, calendar dimension (2020-2030)
    fct_readings.sql        -- incremental, unique_key=['dateTime', 'measure']
    obt_readings.sql        -- table, denormalized join of readings + measures + stations
    agg_daily_readings.sql  -- incremental (merge), daily min/max/median per measure
    station_freshness.sql   -- table, per-station freshness monitoring
    schema.yml              -- model docs, tests, and custom test configs
snapshots/
  snap_stations.sql       -- SCD2 snapshot of stg_stations (check strategy, all columns)
  snap_measures.sql       -- SCD2 snapshot of stg_measures (check strategy, all columns)
packages.yml              -- dbt package dependencies (dbt-utils)
orchestration/
  definitions.py          -- Dagster orchestration (assets, schedules, jobs)
```

Materialization defaults are set in `dbt_project.yml` (both staging and marts = table). Per-model overrides use `{{ config() }}` blocks when needed.

### Layer Responsibilities

- **Staging**: minimal transformation -- unnest JSON, strip URL prefixes (via `strip_api_url` macro). Keep raw types to avoid ingest failures.
- **Marts**: type casting, business logic, aggregations. CAST failures here are safe because raw data is preserved in staging.

## Current Progress

Phase 1 complete:
- [x] Staging models: stg_stations, stg_measures, stg_readings (all working)
- [x] Dimension models: dim_measures, dim_stations (working, with type casting)
- [x] Fact model: fct_readings (incremental)
- [x] Enriched/denormalized model: obt_readings (LEFT JOIN, table materialization)
- [x] Daily summary: agg_daily_readings (incremental with merge, from obt_readings)
- [x] dbt tests (PK unique/not_null, FK relationships with warn/error thresholds)
- [x] Schema definitions (staging and marts schema.yml with consumer-facing descriptions)

Phase 2 complete:
- [x] Snapshots: snap_stations, snap_measures (SCD2, check strategy on all columns)
- [x] Dimensions rewired to read from snapshots with `WHERE dbt_valid_to IS NULL`
- [x] Verified SCD2 behavior: changes create new versions, old versions get end-dated

Phase 3 complete:
- [x] dim_date: calendar dimension built with DuckDB range() function (2020-2030)
- [x] Packages: dbt-utils installed, learned date_spine macro and how to wrap CTE-generating macros
- [x] Sources: raw tables (raw_stations, raw_measures, raw_readings) loaded via `dbt run-operation load_raw_data`
- [x] Sources defined in sources.yml, staging models now use `{{ source() }}` instead of direct URLs
- [x] Source freshness: configured with `_latest_reading_at` column, different thresholds per source type
- [x] Station freshness monitoring: station_freshness model tracks per-station staleness
- [x] Custom generic test: `max_pct_failing` macro for threshold-based aggregate tests (config in YAML, logic in macro)
- [x] Test patterns: singular vs generic tests, `--store-failures`, meaningful failure output

Phase 4 complete:
- [x] Unit tests: tested fct_readings value extraction logic with mock inputs, learned `overrides.macros.is_incremental` for incremental models
- [x] Mockable timestamps: `current_ts` macro for testing time-dependent logic (station_freshness)
- [x] Contracts: enforced schema on dim_stations (column names, types, constraints), learned contracts vs tests trade-offs
- [x] Split YAML: one .yml file per model for cleaner git history

Phase 5 complete:
- [x] Orchestration: Dagster with dagster-dbt integration
- [x] Asset-based model: dbt models appear as Dagster assets with lineage
- [x] Raw data ingestion: runs via subprocess before dbt build (DuckDB single-writer workaround)
- [x] Schedule: 15-minute refresh configured and running
- [x] Local UI: http://localhost:3000 for visibility, manual triggers, logs

Remaining topics on the learning roadmap:
- Backfill (historical CSV data from archive)

## Code Style

- SQL keywords in UPPERCASE (SELECT, FROM, WHERE, etc.)
- Put primary key column first in SELECT lists
- Only prefix column aliases where names collide across joined tables (e.g., `m_label`, `s_label`)

## Tutoring Style

- Be direct with recommendations. Don't argue against sound design choices -- if the user's suggestion is better, agree upfront.
- Don't default to "simpler" when the user proposes a correct and more robust approach.
- User drives decisions; Claude explains concepts and writes the boilerplate SQL.
- Don't box-tick. When teaching a concept (e.g. documentation, tests), explain the *purpose and audience* first, not just the syntax. Schema docs are for data consumers, not developers -- describe what the data means in the real world, not implementation details like "cast to double".
- Apply the same standard to your own output. If you're generating docs, tests, or config, make sure it meets the quality bar you'd teach -- don't just produce something that passes validation.
- Never fabricate example values in documentation. Either verify from the actual data or leave it out. Made-up examples are worse than no examples.
- **Don't skip details or rush to the next topic.** Cover each concept thoroughly before moving on. If something is "worth knowing about", teach it properly -- don't mention it and immediately dismiss it as "not essential for now."
