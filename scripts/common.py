from __future__ import annotations
from pathlib import Path
import polars as pl

EPS = 0.02  # 2¢ tolerance for rounding/float noise

# Canonical column names used during initial normalization (Bronze build).
# Silver reads the *Bronze contract* and does not need to re-canonicalize.
CANON = {
    # datetime
    "tpep_pickup_datetime": "pickup_at",
    "tpep_dropoff_datetime": "dropoff_at",

    # ids
    "VendorID": "vendor_id",
    "vendor_id": "vendor_id",
    "payment_type": "payment_type",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",

    # numerics (as seen in raw TLC files)
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",  # normalized in Bronze, later renamed to *_mi
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
    "total_amount": "total_amount",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "ratecodeid": "rate_code_id",
    "RatecodeID": "rate_code_id",
}

MONEY_COLS = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "total_amount",
]

REQUIRED_FOR_ROW = [
    "pickup_at",
    "dropoff_at",
    "vendor_id",
    "payment_type",
    # distance field is `trip_distance` *before* Bronze rename to `_mi`
    "trip_distance",
    "fare_amount",
    "total_amount",
]


def near(a: pl.Expr, b: pl.Expr | float, eps: float = EPS) -> pl.Expr:
    b_expr = b if isinstance(b, pl.Expr) else pl.lit(b)
    return (a - b_expr).abs() <= eps


def _to_datetime_us(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Make `col` a Datetime(us) regardless of current dtype."""
    if col not in df.columns:
        return df
    dtype = df.schema[col]
    if dtype == pl.Datetime("us"):
        return df  # already correct
    if dtype == pl.Date:
        return df.with_columns(pl.col(col).cast(pl.Datetime("us"), strict=False).alias(col))
    # Anything else (including Utf8 / Int / Unknown) → cast to string then parse
    return df.with_columns(
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .str.strptime(pl.Datetime("us"), strict=False, exact=False)
        .alias(col)
    )


def canonicalize(df: pl.DataFrame) -> pl.DataFrame:
    """
    Used in Bronze build *before* we rename distance to trip_distance_mi.
    """
    # rename known columns; preserve others
    rename_map = {c: CANON[c] for c in df.columns if c in CANON}
    out = df.rename(rename_map)

    # robust datetime handling
    out = _to_datetime_us(out, "pickup_at")
    out = _to_datetime_us(out, "dropoff_at")

    # cast numerics (lenient)
    for c in ["vendor_id", "payment_type", "rate_code_id", "passenger_count", "pu_location_id", "do_location_id"]:
        if c in out.columns:
            out = out.with_columns(pl.col(c).cast(pl.Int64, strict=False))

    for c in ["trip_distance"] + MONEY_COLS:
        if c in out.columns:
            out = out.with_columns(pl.col(c).cast(pl.Float64, strict=False))

    # ensure present money columns
    for c in MONEY_COLS:
        if c not in out.columns:
            out = out.with_columns(pl.lit(None).cast(pl.Float64).alias(c))

    # assure store_and_fwd_flag is string when present
    if "store_and_fwd_flag" in out.columns:
        out = out.with_columns(pl.col("store_and_fwd_flag").cast(pl.Utf8, strict=False))

    return out


def add_dup_key(df: pl.DataFrame) -> pl.DataFrame:
    """
    General-purpose dedup key (used outside Option B Bronze if needed).
    Prefers `trip_distance_mi` when present; otherwise falls back to `trip_distance`.
    Uses native hashing; no Python UDF unless absolutely necessary.
    """
    dist_col = "trip_distance_mi" if "trip_distance_mi" in df.columns else "trip_distance"
    key_cols = [
        "vendor_id", "pickup_at", "dropoff_at", "passenger_count",
        dist_col, "fare_amount", "total_amount", "payment_type",
    ]
    for c in key_cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))
    try:
        return df.with_columns(pl.hash([pl.col(c) for c in key_cols], seed=42).alias("dup_key"))
    except Exception:
        try:
            return df.with_columns(pl.struct([pl.col(c) for c in key_cols]).hash(seed=42).alias("dup_key"))
        except Exception:
            import hashlib
            return df.with_columns(
                pl.concat_str([pl.col(c).cast(pl.Utf8) for c in key_cols])
                .map_elements(lambda s: hashlib.sha1(s.encode("utf-8")).hexdigest(), return_dtype=pl.Utf8)
                .alias("dup_key")
            )


def fare_components_expr() -> pl.Expr:
    return (
        pl.coalesce(pl.col("fare_amount"), pl.lit(0.0))
        + pl.coalesce(pl.col("extra"), pl.lit(0.0))
        + pl.coalesce(pl.col("mta_tax"), pl.lit(0.0))
        + pl.coalesce(pl.col("tip_amount"), pl.lit(0.0))
        + pl.coalesce(pl.col("tolls_amount"), pl.lit(0.0))
        + pl.coalesce(pl.col("improvement_surcharge"), pl.lit(0.0))
        + pl.coalesce(pl.col("congestion_surcharge"), pl.lit(0.0))
        + pl.coalesce(pl.col("airport_fee"), pl.lit(0.0))
        + pl.coalesce(pl.col("cbd_congestion_fee"), pl.lit(0.0))
    )


def classify_rows(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    """
    Silver classification for Option B:
      - Assumes Bronze emitted `trip_distance_mi` and QA flags.
      - Coalesces all input QA flags to False to avoid tri-valued logic.
      - Emits:
          is_rejected_missing, is_admin_issue, is_fare_mismatch,
          is_anomaly (operational; EXCLUDES fare mismatches)
    """
    dist = pl.col("trip_distance_mi")

    # Coalesce QA flags from Bronze
    qa_in_window     = pl.col("qa_in_file_window").fill_null(False)
    qa_out_dist      = pl.col("qa_outlier_distance").fill_null(False)
    qa_out_speed     = pl.col("qa_outlier_speed").fill_null(False)
    qa_adj           = pl.col("qa_is_adjustment").fill_null(False)
    qa_fare_mismatch = pl.col("qa_is_fare_mismatch").fill_null(False)

    # Deterministic booleans
    is_missing_crit = (
        pl.col("pickup_at").is_null()
        | pl.col("dropoff_at").is_null()
        | dist.is_null()
        | pl.col("total_amount").is_null()
        | (pl.col("pu_location_id").is_null() | (pl.col("pu_location_id") <= 0))
        | (pl.col("do_location_id").is_null() | (pl.col("do_location_id") <= 0))
    ).alias("is_rejected_missing")

    is_admin_issue   = qa_adj.alias("is_admin_issue")
    is_fare_mismatch = qa_fare_mismatch.alias("is_fare_mismatch")

    # Operational anomalies (exclude mismatches)
    is_anomaly = (
        ((~qa_in_window) | qa_out_dist | qa_out_speed) & (~qa_fare_mismatch)
    ).alias("is_anomaly")

    return (
        df.with_columns([
            is_missing_crit,
            is_admin_issue,
            is_fare_mismatch,
            is_anomaly,
        ])
    )

def ensure_dirs(*paths: Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)
