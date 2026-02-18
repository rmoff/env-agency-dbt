# Environment Agency Flood Data Pipeline

A dbt project that ingests and transforms UK Environment Agency flood monitoring data. Built as a learning exercise [guided by Claude Code](CLAUDE.md) to understand dbt patterns with real-world messy data.

This started as a blog post about [building a data pipeline with DuckDB](https://rmoff.net/2025/03/20/building-a-data-pipeline-with-duckdb/). The dbt version applies proper patterns: staging/marts separation, incremental loading, SCD2 snapshots, and actual tests.

## What it does

Pulls data from the [Environment Agency real-time flood API](https://environment.data.gov.uk/flood-monitoring/doc/reference):

- **Stations** — monitoring stations around the UK
- **Measures** — what each station tracks (river level, rainfall, etc.)
- **Readings** — the actual measurements

The pipeline stages the raw JSON, builds SCD2 snapshots for dimensions, loads readings incrementally, and produces a denormalized "one big table" for analysis.

## Data model

```
readings (fact) --> measures (dim) --> stations (dim)
```

Readings reference measures by ID, measures reference stations. Both dimension tables are snapshotted to preserve history. Two versions of `dim_date` to learn about different ways of building it.

![](lineage.png)

## Setup

Requires Python 3.13+ and DuckDB.

```bash
# Create venv and install deps
uv venv
source .venv/bin/activate
uv pip install dbt-duckdb dagster dagster-dbt dagster-webserver

# Install dbt packages
dbt deps

# Check connection
dbt debug
```

Configure your DuckDB path in `~/.dbt/profiles.yml`:

```yaml
env_agency:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /path/to/your/database.duckdb
```

## Running the pipeline

### Manual runs

```bash
# Load fresh data from API
dbt run-operation load_raw_readings
dbt run-operation load_raw_stations
dbt run-operation load_raw_measures

# Build everything
dbt build

# Or just specific layers
dbt run --select staging
dbt run --select marts
```

### Backfill historical data

The API only returns recent readings. For historical data, use the CSV archive:

```bash
dbt run-operation backfill_readings --args '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
dbt build --full-refresh --select stg_readings_archive fct_readings+
```

### Scheduled runs (Dagster)

```bash
cd orchestration
dagster dev
```

Opens at http://localhost:3000. A 15-minute schedule is configured to refresh data.

## Project structure

```
models/
  staging/          # Raw data with minimal transforms
    stg_stations.sql
    stg_measures.sql
    stg_readings.sql
    stg_readings_archive.sql   # Historical CSV data
  marts/            # Business-ready models
    dim_stations.sql
    dim_measures.sql
    dim_date.sql
    fct_readings.sql           # Incremental
    obt_readings.sql           # Denormalized join
    agg_daily_readings.sql     # Daily stats
    station_freshness.sql      # Monitoring

snapshots/          # SCD2 history for dimensions
  snap_stations.sql
  snap_measures.sql

macros/
  ingestion/        # API and CSV loading
  strip_api_url.sql # FK cleanup (API returns full URLs)

orchestration/
  definitions.py    # Dagster assets and schedules
```

## Data quirks

The Environment Agency data has some edge cases:

- Foreign keys arrive as full URLs (`http://environment.data.gov.uk/flood-monitoring/id/measures/...`) — stripped in staging
- Some fields randomly return JSON arrays instead of scalars — we take the first element
- Historical CSV has pipe-delimited values in ~0.01% of rows (`0.770|0.688`) — we take the first value
- Stations report at different frequencies, so "freshness" varies by station

## Tests

```bash
dbt test                    # Run all tests
dbt test --select marts     # Just mart tests
dbt build                   # Run + test in dependency order
```

Tests cover primary keys, foreign key relationships, and data quality thresholds.

## Documentation

```bash
dbt docs generate           # Build the docs site
dbt docs serve              # Serve at http://localhost:8080
```

Includes model descriptions, column documentation, and an interactive lineage graph.
