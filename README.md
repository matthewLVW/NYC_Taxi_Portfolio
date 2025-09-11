# NYC Taxi — Local DuckDB + dbt Core MVP

A self-contained local analytics pipeline that runs end-to-end on a laptop.

Flow: Bronze/Silver in Python (Polars) → Gold/Marts in DuckDB via dbt → optional Streamlit dashboard.

---

## Repo Layout

```
data/                 # raw + bronze + silver Parquet
scripts/              # bronze_build.py, silver_split.py (Polars)
db/                   # DuckDB database file
dbt/                  # dbt Core project (DuckDB)
  models/
    staging/          # views over Silver Parquet
    gold/             # star schema (facts/dims)
    marts/            # curated aggregates
  seeds/              # vendor/payment lookups
  macros/tests/       # custom data tests
app/                  # optional Streamlit demo
```

## Quickstart (Windows PowerShell and Unix shells)

Requires Python 3.10+

```bash
# 1) Create venv & install deps
python -m venv .venv
# Windows: .venv\Scripts\activate
# Unix:    source .venv/bin/activate
pip install -r requirements.txt

# 2) Point dbt at the project-local profiles.yml
# PowerShell
$env:DBT_PROFILES_DIR = (Resolve-Path "dbt").Path
# bash/zsh
export DBT_PROFILES_DIR="$(pwd)/dbt"

# 3) Place TLC Parquet(s) into data/raw/
#    e.g., yellow_tripdata_2024-01.parquet

# 4) Build Bronze and Silver
python scripts/bronze_build.py --raw-dir data/raw --out data/bronze/bronze_trips.parquet
python scripts/silver_split.py --bronze data/bronze/bronze_trips.parquet --outdir data/silver

# 5) Build Gold & Marts in DuckDB via dbt
dbt deps   # safe even if no packages
dbt seed   # load vendors/payment types
dbt run    # builds staging, gold, marts
dbt test   # runs tests (including custom fare tolerance)
dbt docs generate
# dbt docs serve  # optional local docs site

# 6) (Optional) Dashboard
streamlit run app/execdashboard.py
```

## Makefile shortcuts

```bash
make setup   # create venv and install dependencies
make bronze  # run Bronze
make silver  # run Silver
make gold    # dbt deps + seed + run + test + docs
make demo    # bronze + silver + dbt build/tests
```

## Configuration

- DuckDB path: `db/warehouse.duckdb` (see `dbt/profiles.yml`).
- Silver directory: `data/silver` (used by dbt external sources).
- Fare reconciliation tolerance is `0.02` (2¢) in Python and the dbt custom test.

## Star Schema (Gold)

- fact_trips: one row per trip with vendor/payment/date foreign keys.
- fact_revenue_adjustments: rows where `components_total` and `total_amount` diverge.
- dim_date, dim_vendor, dim_payment, dim_zone.

## Notes

- Designed for portability: local files only; no external warehouse.
- `scripts/common.py` centralizes canonical naming, type coercion, and QA logic.
- Uses lazy scans to keep memory bounded on larger months.
- Extend with additional dbt tests or marts as desired.

## License

Specify a license for your repo (e.g., MIT, Apache 2.0) in a `LICENSE` file.
