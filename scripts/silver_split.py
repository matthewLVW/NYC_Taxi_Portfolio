from __future__ import annotations
import argparse
from pathlib import Path
import polars as pl


def _write_partition(lf: pl.LazyFrame, out_path: Path) -> None:
    """
    Write a LazyFrame to Parquet without materializing everything in memory.
    Prefer sink_parquet; fall back to collect(engine="streaming").
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        lf.sink_parquet(out_path.as_posix())
    except Exception:
        lf.collect(engine="streaming").write_parquet(
            out_path.as_posix(), compression="zstd", statistics=True
        )


def _count_rows(parquet_path: Path) -> int:
    if not parquet_path.exists():
        return 0
    return (
        pl.scan_parquet(parquet_path.as_posix())
        .select(pl.len())
        .collect()
        .item()
    )


def split_silver(bronze_file: Path, silver_dir: Path) -> None:
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Lazy scan of Bronze
    lf = pl.scan_parquet(bronze_file.as_posix())

    # ---- Enforce Bronze contract (column names) ----
    EXPECTED = {
        # core contract columns
        "vendor_id", "pickup_at", "dropoff_at", "passenger_count",
        "trip_distance_mi", "rate_code_id", "store_and_fwd_flag",
        "pu_location_id", "do_location_id", "payment_type",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "congestion_surcharge", "airport_fee",
        "cbd_congestion_fee", "total_amount",
        # derived from Bronze
        "manualTotal", "duration_min", "speed_mph",
        # QA flags from Bronze
        "qa_in_file_window", "qa_outlier_distance", "qa_outlier_speed",
        "qa_is_fare_mismatch", "qa_is_adjustment",
        # dup & lineage
        "dup_key", "qa_is_duplicate_in_file", "source_year", "source_month", "source_file",
    }
    sch = lf.collect_schema()
    names = set(sch.names())
    missing = sorted(EXPECTED - names)
    extra   = sorted(names - EXPECTED)
    if missing or extra:
        raise SystemExit(
            "Bronze schema mismatch.\n"
            f"Missing columns: {missing}\n"
            f"Extra columns:   {extra}\n"
            "If you changed Bronze, rebuild Bronze before running Silver."
        )

    # ---- Normalize dtypes we depend on, and coalesce all QA flags to strict booleans
    lf = lf.with_columns([
        pl.col("pickup_at").cast(pl.Datetime("us"), strict=False),
        pl.col("dropoff_at").cast(pl.Datetime("us"), strict=False),
        pl.col("pu_location_id").cast(pl.Int32, strict=False),
        pl.col("do_location_id").cast(pl.Int32, strict=False),
        pl.col("trip_distance_mi").cast(pl.Float64, strict=False),
        pl.col("total_amount").cast(pl.Float64, strict=False),

        pl.col("qa_in_file_window").fill_null(False).cast(pl.Boolean),
        pl.col("qa_outlier_distance").fill_null(False).cast(pl.Boolean),
        pl.col("qa_outlier_speed").fill_null(False).cast(pl.Boolean),
        pl.col("qa_is_adjustment").fill_null(False).cast(pl.Boolean),
        pl.col("qa_is_fare_mismatch").fill_null(False).cast(pl.Boolean),
    ])

    # ------------------------------ RULE FLAGS -----------------------------------
    is_missing_crit = (
        pl.col("pickup_at").is_null()
        | pl.col("dropoff_at").is_null()
        | pl.col("trip_distance_mi").is_null()
        | pl.col("total_amount").is_null()
        | (pl.col("pu_location_id").is_null() | (pl.col("pu_location_id") <= 0))
        | (pl.col("do_location_id").is_null() | (pl.col("do_location_id") <= 0))
    )

    is_admin    = pl.col("qa_is_adjustment")
    is_anomaly  = (
        (~pl.col("qa_in_file_window"))
        | pl.col("qa_outlier_distance")
        | pl.col("qa_outlier_speed")
    ) & (~pl.col("qa_is_fare_mismatch"))

    # ----------------------------- PARTITIONS ------------------------------------
    base_good   = (~is_missing_crit) & (~is_admin)

    lf_rejected = lf.filter(is_missing_crit)
    lf_admin    = lf.filter((~is_missing_crit) & is_admin)
    lf_anom     = lf.filter(base_good & is_anomaly)
    lf_clean    = lf.filter(base_good & (~is_anomaly))                  # includes fare mismatches
    lf_fmiss    = lf_clean.filter(pl.col("qa_is_fare_mismatch"))        # subset of clean

    # Optional: drop any extra fare-related flags from earlier experiments
    def drop_unwanted(lf_part: pl.LazyFrame) -> pl.LazyFrame:
        return lf_part.select(pl.all().exclude(["qa_is_fee_misflag", "fee_misflag_delta"]))

    lf_rejected  = drop_unwanted(lf_rejected)
    lf_admin     = drop_unwanted(lf_admin)
    lf_anom      = drop_unwanted(lf_anom)
    lf_clean     = drop_unwanted(lf_clean)
    lf_fmiss     = drop_unwanted(lf_fmiss)

    # ------------------------------- SINK TO PARQUET ------------------------------
    out_rej   = silver_dir / "silver.rejected.parquet"
    out_admin = silver_dir / "silver.trips_admin.parquet"
    out_anom  = silver_dir / "silver.trips_anomalies.parquet"
    out_clean = silver_dir / "silver.trips_clean.parquet"
    out_fmiss = silver_dir / "silver.trips_fare_miss.parquet"

    _write_partition(lf_rejected, out_rej)
    _write_partition(lf_admin,    out_admin)
    _write_partition(lf_anom,     out_anom)
    _write_partition(lf_clean,    out_clean)
    _write_partition(lf_fmiss,    out_fmiss)

    # ------------------------------- REPORT ---------------------------------------
    total_rows = int(lf.select(pl.len()).collect().item())
    n_rej      = _count_rows(out_rej)
    n_adm      = _count_rows(out_admin)
    n_anom     = _count_rows(out_anom)
    n_clean    = _count_rows(out_clean)
    n_fmiss    = _count_rows(out_fmiss)

    # Coverage sanity (ignoring fmiss overlap): base_good must equal clean + anom
    base_good_count = total_rows - n_rej - n_adm
    covered = n_clean + n_anom
    if covered != base_good_count:
        print(f"⚠️  Coverage check: clean+anom={covered:,} vs base_good={base_good_count:,} (diff={base_good_count - covered:,})")

    print("=== SILVER SPLIT COMPLETE (streaming, no dedup) ===")
    print(f"Rejected            -> {out_rej}        rows={n_rej:,}")
    print(f"Trips (Admin)       -> {out_admin}     rows={n_adm:,}")
    print(f"Trips (Anomalies)   -> {out_anom}      rows={n_anom:,}")
    print(f"Trips (Clean)       -> {out_clean}     rows={n_clean:,}")
    print(f"Trips (Fare Miss)   -> {out_fmiss}  rows={n_fmiss:,}  (subset of Clean)")
    print(f"\nBronze rows input (from parquet): {total_rows:,}")
    print(f"Sum of Silver partitions (with overlap): {n_rej + n_adm + n_anom + n_clean + n_fmiss:,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", default="data/bronze/bronze_trips.parquet", type=Path)
    ap.add_argument("--outdir", default="data/silver", type=Path)
    args = ap.parse_args()
    split_silver(args.bronze, args.outdir)
