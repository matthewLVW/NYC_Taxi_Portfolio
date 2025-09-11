from __future__ import annotations
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import polars as pl
import pyarrow.parquet as pq

from common import canonicalize, ensure_dirs  # we won't use add_dup_key; we build dup_key here

# -------------------------- Data Contract (names & dtypes) --------------------------

CONTRACT_DTYPES: dict[str, pl.DataType] = {
    "vendor_id": pl.Int16,
    "pickup_at": pl.Datetime("us"),
    "dropoff_at": pl.Datetime("us"),
    "passenger_count": pl.Int16,
    "trip_distance_mi": pl.Float64,
    "rate_code_id": pl.Int16,
    "store_and_fwd_flag": pl.Utf8,
    "pu_location_id": pl.Int32,
    "do_location_id": pl.Int32,
    "payment_type": pl.Int16,
    "fare_amount": pl.Float64,
    "extra": pl.Float64,
    "mta_tax": pl.Float64,
    "tip_amount": pl.Float64,
    "tolls_amount": pl.Float64,
    "improvement_surcharge": pl.Float64,
    "total_amount": pl.Float64,
    "congestion_surcharge": pl.Float64,
    "airport_fee": pl.Float64,
    "cbd_congestion_fee": pl.Float64,

    # derived
    "manualTotal": pl.Float64,
    "duration_min": pl.Float64,
    "speed_mph": pl.Float64,

    # QA flags
    "qa_in_file_window": pl.Boolean,
    "qa_outlier_distance": pl.Boolean,
    "qa_outlier_speed": pl.Boolean,
    "qa_is_fare_mismatch": pl.Boolean,
    "qa_is_adjustment": pl.Boolean,

    # dup & lineage
    "dup_key": pl.Utf8,
    "qa_is_duplicate_in_file": pl.Boolean,
    "source_year": pl.Int32,
    "source_month": pl.Int32,
    "source_file": pl.Utf8,
}

CONTRACT_COLS: List[str] = list(CONTRACT_DTYPES.keys())

MONEY_COLS = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "airport_fee",
    "cbd_congestion_fee",
]

# ------------------------------- Helpers -----------------------------------------

def parse_year_month_from_filename(name: str) -> Tuple[int, int]:
    # expects e.g. yellow_tripdata_2024-01.parquet
    m = re.search(r"(\d{4})-(\d{2})", name)
    if not m:
        raise ValueError(f"Cannot parse year-month from file name: {name}")
    return int(m.group(1)), int(m.group(2))

def file_window(year: int, month: int) -> Tuple[datetime, datetime]:
    start = datetime(year, month, 1)
    next_start = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    return (start - timedelta(days=2)), (next_start + timedelta(days=2))

def ensure_money_columns(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    for c in MONEY_COLS:
        if c not in out.columns:
            out = out.with_columns(pl.lit(0.0).alias(c))
    return out

def compute_derivations_and_flags(df: pl.DataFrame, yr: int, mo: int) -> pl.DataFrame:
    """Add manualTotal, duration_min, speed_mph, QA flags per contract."""
    win_lo, win_hi = file_window(yr, mo)

    # duration (min) and speed (mph)
    df = df.with_columns(
        ((pl.col("dropoff_at").cast(pl.Int64) - pl.col("pickup_at").cast(pl.Int64)) / 60_000_000)
        .alias("duration_min")
    ).with_columns(
        pl.when(pl.col("duration_min") > 0)
          .then(60.0 * pl.col("trip_distance_mi") / pl.col("duration_min"))
          .otherwise(None)
          .alias("speed_mph")
    )

    # arithmetic components total
    df = df.with_columns(
        (
            pl.col("fare_amount").fill_null(0.0)
          + pl.col("extra").fill_null(0.0)
          + pl.col("mta_tax").fill_null(0.0)
          + pl.col("tolls_amount").fill_null(0.0)
          + pl.col("improvement_surcharge").fill_null(0.0)
          + pl.col("congestion_surcharge").fill_null(0.0)
          + pl.col("airport_fee").fill_null(0.0)
          + pl.col("cbd_congestion_fee").fill_null(0.0)
          + pl.col("tip_amount").fill_null(0.0)
        ).alias("manualTotal")
    )

    # QA flags
    df = df.with_columns([
        (pl.col("pickup_at").is_between(win_lo, win_hi)
         & pl.col("dropoff_at").is_between(win_lo, win_hi)).alias("qa_in_file_window"),

        (~pl.col("trip_distance_mi").is_between(0, 150, closed="right")).alias("qa_outlier_distance"),

        ((pl.col("duration_min") < 1)
         | (pl.col("duration_min") > 360)
         | (pl.col("speed_mph") > 80)).alias("qa_outlier_speed"),

        ((pl.col("manualTotal") - pl.col("total_amount")).abs() > 0.50).alias("qa_is_fare_mismatch"),

        (
            (pl.col("total_amount") < 0)
            | (pl.col("fare_amount") < 0)
            | (pl.col("extra") < 0)
            | (pl.col("mta_tax") < 0)
            | (pl.col("tolls_amount") < 0)
            | (pl.col("improvement_surcharge") < 0)
            | (pl.col("congestion_surcharge") < 0)
            | (pl.col("airport_fee") < 0)
            | (pl.col("cbd_congestion_fee") < 0)
            | (pl.col("payment_type").is_in([3, 4, 6]))  # no-charge, dispute, voided
        ).alias("qa_is_adjustment"),
    ])
    df = df.with_columns([
    pl.col("qa_in_file_window").fill_null(False).cast(pl.Boolean),
    pl.col("qa_outlier_distance").fill_null(False).cast(pl.Boolean),
    pl.col("qa_outlier_speed").fill_null(False).cast(pl.Boolean),
    pl.col("qa_is_fare_mismatch").fill_null(False).cast(pl.Boolean),
    pl.col("qa_is_adjustment").fill_null(False).cast(pl.Boolean),
])
    return df

def build_dup_key(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build dup_key using contract fields (use distance in miles).
    Avoid Python UDFs; rely on native hashing with stable seed. Emit as hex-like string.
    """
    key_cols = [
        "vendor_id", "pickup_at", "dropoff_at",
        "pu_location_id", "do_location_id",
        "fare_amount", "trip_distance_mi",
    ]
    for c in key_cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))

    # Prefer native pl.hash(); fallback to struct().hash()
    try:
        hashed = pl.hash([pl.col(c) for c in key_cols], seed=42)
    except Exception:
        hashed = pl.struct([pl.col(c) for c in key_cols]).hash(seed=42)

    # cast UInt64 -> Utf8 so it's easy to diff across systems (not strictly required)
    return df.with_columns(hashed.cast(pl.Utf8).alias("dup_key"))

def cast_and_select_contract(df: pl.DataFrame) -> pl.DataFrame:
    # add any missing columns, then cast types and select in canonical order
    for c, dt in CONTRACT_DTYPES.items():
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dt).alias(c))
    return df.select([pl.col(c).cast(CONTRACT_DTYPES[c], strict=False).alias(c) for c in CONTRACT_COLS])

# --------------------------------- Bronze Build ------------------------------------

def build_bronze(raw_dir: Path, bronze_out: Path) -> None:
    ensure_dirs(bronze_out.parent)

    files = sorted(Path(raw_dir).glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {raw_dir}. Place TLC parquet(s) here.")

    print(f"🗂️  Found {len(files)} raw parquet file(s) under {raw_dir}")

    # Overwrite target if exists to avoid schema conflicts
    if bronze_out.exists():
        bronze_out.unlink()

    writer: pq.ParquetWriter | None = None
    total_in = 0
    total_written = 0
    total_dups = 0

    for f in files:
        print(f"\n📦 Processing {f.name} ...")
        yr, mo = parse_year_month_from_filename(f.name)

        # Eager read per-file, add lineage
        df = pl.read_parquet(f.as_posix())
        df = df.with_columns(pl.lit(f.name).alias("source_file"))
        n_in = df.height
        total_in += n_in
        print(f"   ↳ loaded {n_in:,} rows")

        # Canonicalize names/dtypes (produces `trip_distance`); ensure money cols exist
        df = canonicalize(df)
        df = ensure_money_columns(df)

        # Rename to contract distance name
        if "trip_distance_mi" not in df.columns and "trip_distance" in df.columns:
            df = df.rename({"trip_distance": "trip_distance_mi"})

        # Enforce required base dtypes (narrow ints to contract)
        df = df.with_columns([
            pl.col("vendor_id").cast(pl.Int16, strict=False),
            pl.col("passenger_count").cast(pl.Int16, strict=False),
            pl.col("rate_code_id").cast(pl.Int16, strict=False),
            pl.col("payment_type").cast(pl.Int16, strict=False),
            pl.col("pu_location_id").cast(pl.Int32, strict=False),
            pl.col("do_location_id").cast(pl.Int32, strict=False),
            pl.col("pickup_at").cast(pl.Datetime("us"), strict=False),
            pl.col("dropoff_at").cast(pl.Datetime("us"), strict=False),
            pl.col("store_and_fwd_flag").cast(pl.Utf8, strict=False),
            pl.col("trip_distance_mi").cast(pl.Float64, strict=False),
            *[pl.col(c).cast(pl.Float64, strict=False) for c in MONEY_COLS + ["total_amount"]],
        ])

        # Derivations & QA flags
        df = compute_derivations_and_flags(df, yr, mo)

        # Per-file duplicate marking + dedup (keep first by dup_key / time / file)
        df = build_dup_key(df)
        df = df.sort(["dup_key", "pickup_at", "dropoff_at", "source_file"], nulls_last=True)
        df = df.with_columns(pl.col("dup_key").is_duplicated().alias("qa_is_duplicate_in_file"))
        removed = int(df["qa_is_duplicate_in_file"].sum())
        df = df.filter(~pl.col("qa_is_duplicate_in_file"))
        total_dups += removed
        print(f"   ↳ dedup within file: removed {removed:,} dup(s); kept {df.height:,}")

        # Lineage
        df = df.with_columns([
            pl.lit(yr).alias("source_year"),
            pl.lit(mo).alias("source_month"),
        ])

        # Cast/select to exact contract
        df = cast_and_select_contract(df)

        # Append to Parquet with stable schema
        table = df.to_arrow()
        if writer is None:
            writer = pq.ParquetWriter(
                where=str(bronze_out),
                schema=table.schema,
                compression="zstd",
            )
        writer.write_table(table)
        total_written += df.height

    if writer is not None:
        writer.close()

    size = bronze_out.stat().st_size if bronze_out.exists() else 0
    print("\n=== BRONZE BUILD COMPLETE (contract) ===")
    print(f"Unified Parquet : {bronze_out.resolve()}")
    print(f"Rows input      : {total_in:,}")
    print(f"Rows written    : {total_written:,}")
    print(f"Dups removed    : {total_dups:,}")
    print(f"File size       : {size:,} bytes")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default="data/raw", type=Path, help="Folder containing monthly TLC parquet files.")
    ap.add_argument("--out", default="data/bronze/bronze_trips.parquet", type=Path, help="Output Bronze parquet path.")
    args = ap.parse_args()
    build_bronze(args.raw_dir, args.out)
