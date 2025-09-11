# tools/export_schema_text.py
from __future__ import annotations

import argparse
import datetime as dt
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import duckdb


# -----------------------------
# Configuration / heuristics
# -----------------------------
# Classify tables by name prefix
TYPE_PREFIX = {
    "dim_": "DIMENSION",
    "fact_": "FACT",
    "mart_": "MART",
}

# Known FK mappings (column name -> (dimension_base_name, dim_pk_col))
# We'll resolve the actual schema for each dimension at runtime.
FK_MAP = {
    # canonical
    "date_day": ("dim_date", "date_day"),
    "vendor_id": ("dim_vendor", "vendor_id"),
    "payment_type": ("dim_payment", "payment_type"),
    "location_id": ("dim_zone", "location_id"),
    # NYC taxi specifics
    "pu_location_id": ("dim_zone", "location_id"),
    "do_location_id": ("dim_zone", "location_id"),
}

# For tables with these date-ish columns we’ll include a min/max summary.
DATE_COL_CANDIDATES = ("date_day", "date", "ds")


# -----------------------------
# Data classes for YAML-ish dump
# -----------------------------
@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool

@dataclass
class TableStats:
    row_count: int
    date_column: Optional[str] = None
    date_min: Optional[str] = None
    date_max: Optional[str] = None

@dataclass
class Relationship:
    fk_table: str
    fk_column: str
    pk_table: str
    pk_column: str
    confidence: str  # "high" (name match) | "heuristic"

@dataclass
class TableInfo:
    schema: str
    name: str
    type: str  # FACT | DIMENSION | MART | OTHER
    columns: List[ColumnInfo]
    stats: TableStats
    relationships: List[Relationship]


# -----------------------------
# Helpers
# -----------------------------
def detect_table_type(table_name: str) -> str:
    for prefix, label in TYPE_PREFIX.items():
        if table_name.lower().startswith(prefix):
            return label
    return "OTHER"

def list_schemas(con: duckdb.DuckDBPyConnection) -> List[str]:
    q = """
      select distinct table_schema
      from information_schema.tables
      where table_type in ('BASE TABLE', 'VIEW')
      order by 1
    """
    return [r[0] for r in con.execute(q).fetchall()]

def list_tables(con: duckdb.DuckDBPyConnection, schema: str) -> List[str]:
    q = """
      select table_name
      from information_schema.tables
      where table_schema = ? and table_type in ('BASE TABLE','VIEW')
      order by 1
    """
    return [r[0] for r in con.execute(q, [schema]).fetchall()]

def get_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> List[ColumnInfo]:
    q = """
      select column_name, data_type, is_nullable
      from information_schema.columns
      where table_schema = ? and table_name = ?
      order by ordinal_position
    """
    out = []
    for name, dtype, nullable in con.execute(q, [schema, table]).fetchall():
        out.append(ColumnInfo(name=name, type=dtype, nullable=(str(nullable).upper() == "YES")))
    return out

def get_row_count(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    q = f'SELECT COUNT(*)::BIGINT FROM "{schema}"."{table}"'
    return int(con.execute(q).fetchone()[0])

def detect_date_col_in_table(columns: List[ColumnInfo]) -> Optional[str]:
    colnames = {c.name for c in columns}
    for c in DATE_COL_CANDIDATES:
        if c in colnames:
            return c
    return None

def get_date_range(con: duckdb.DuckDBPyConnection, schema: str, table: str, date_col: str) -> Tuple[Optional[str], Optional[str]]:
    q = f'SELECT MIN("{date_col}")::DATE, MAX("{date_col}")::DATE FROM "{schema}"."{table}"'
    try:
        mn, mx = con.execute(q).fetchone()
        def fmt(x):
            if x is None:
                return None
            if isinstance(x, (dt.date, dt.datetime)):
                return str(x)[:10]
            return str(x)
        return fmt(mn), fmt(mx)
    except Exception:
        return None, None

def build_dim_index(tables: List[TableInfo]) -> Dict[str, Tuple[str, str]]:
    """
    Build index of known dimension primary keys by dim base name.
    Returns: { 'dim_vendor': ('schema', 'vendor_id'), ... }
    Heuristic: if a dim has exactly one column that ends with _id or equals date_day/location_id/payment_type/vendor_id,
    treat it as PK. If multiple, prefer canonical names.
    """
    idx: Dict[str, Tuple[str, str]] = {}
    for t in tables:
        if t.type != "DIMENSION":
            continue
        base = t.name  # e.g., dim_vendor
        candidates = [c.name for c in t.columns if c.name.endswith("_id")] + [
            c.name for c in t.columns if c.name in ("date_day", "payment_type", "vendor_id", "location_id")
        ]
        # de-dup keep order
        seen = set()
        candidates = [c for c in candidates if not (c in seen or seen.add(c))]
        # heuristics
        pk = None
        for fav in ("date_day", "vendor_id", "payment_type", "location_id"):
            if fav in candidates:
                pk = fav
                break
        if not pk and candidates:
            pk = candidates[0]
        if pk:
            idx[base] = (t.schema, pk)
    return idx

def infer_relationships(table: TableInfo, dim_index: Dict[str, Tuple[str, str]]) -> List[Relationship]:
    rels: List[Relationship] = []
    for col in table.columns:
        key = col.name
        if key in FK_MAP:
            dim_name, dim_pk = FK_MAP[key]
            if dim_name in dim_index:
                dim_schema, dim_pkname = dim_index[dim_name]
                # allow FK map override to differ from actual PK if needed
                target_pk = dim_pk if dim_pk else dim_pkname
                rels.append(
                    Relationship(
                        fk_table=f"{table.schema}.{table.name}",
                        fk_column=key,
                        pk_table=f"{dim_schema}.{dim_name}",
                        pk_column=target_pk,
                        confidence="high",
                    )
                )
        else:
            # generic heuristic: if there exists a dimension whose PK name equals this column name
            for dim_name, (dim_schema, dim_pkname) in dim_index.items():
                if key == dim_pkname:
                    rels.append(
                        Relationship(
                            fk_table=f"{table.schema}.{table.name}",
                            fk_column=key,
                            pk_table=f"{dim_schema}.{dim_name}",
                            pk_column=dim_pkname,
                            confidence="heuristic",
                        )
                    )
    # de-dup (same fk->pk pair could appear twice)
    dedup = {}
    for r in rels:
        dedup[(r.fk_table, r.fk_column, r.pk_table, r.pk_column)] = r
    return list(dedup.values())


# -----------------------------
# Main export
# -----------------------------
def export_schema(db_path: str, out_path: str) -> None:
    con = duckdb.connect(db_path, read_only=True)
    try:
        con.execute("PRAGMA threads=auto;")
        con.execute("PRAGMA enable_progress_bar=false;")
    except Exception:
        pass

    # Collect table metadata
    all_tables: List[TableInfo] = []
    for schema in list_schemas(con):
        for table in list_tables(con, schema):
            ttype = detect_table_type(table)
            cols = get_columns(con, schema, table)
            rc = get_row_count(con, schema, table)
            date_col = detect_date_col_in_table(cols)
            dmin, dmax = (None, None)
            if date_col:
                dmin, dmax = get_date_range(con, schema, table, date_col)
            tstats = TableStats(
                row_count=rc,
                date_column=date_col,
                date_min=dmin,
                date_max=dmax,
            )
            all_tables.append(
                TableInfo(
                    schema=schema,
                    name=table,
                    type=ttype,
                    columns=cols,
                    stats=tstats,
                    relationships=[],
                )
            )

    # Build dimension index and infer relationships for facts/marts
    dim_index = build_dim_index(all_tables)
    for t in all_tables:
        if t.type in ("FACT", "MART"):
            t.relationships = infer_relationships(t, dim_index)

    # Emit a YAML-like, LLM-friendly text
    # (No external deps; hand-roll indentation.)
    lines: List[str] = []
    lines.append("database:")
    lines.append(f"  path: {db_path}")
    lines.append("  exported_at_utc: " + dt.datetime.utcnow().isoformat() + "Z")
    lines.append("  notes: >-")
    lines.append("    Text-based ERD export for LLM consumption. Tables are grouped by schema.")
    lines.append("    Columns include nullability and DuckDB logical types. Row counts and")
    lines.append("    date ranges are provided when available. Relationships are inferred")
    lines.append("    heuristically based on naming and known dimensions.")
    lines.append("schemas:")

    # Group by schema
    by_schema: Dict[str, List[TableInfo]] = {}
    for t in all_tables:
        by_schema.setdefault(t.schema, []).append(t)

    for schema, tables in sorted(by_schema.items()):
        lines.append(f"  - name: {schema}")
        lines.append(f"    tables:")
        for t in sorted(tables, key=lambda x: (x.type, x.name)):
            lines.append(f"      - name: {t.name}")
            lines.append(f"        type: {t.type}")
            lines.append(f"        row_count: {t.stats.row_count}")
            if t.stats.date_column:
                lines.append(f"        date_profile:")
                lines.append(f"          column: {t.stats.date_column}")
                lines.append(f"          min: {t.stats.date_min}")
                lines.append(f"          max: {t.stats.date_max}")
            lines.append(f"        columns:")
            for c in t.columns:
                lines.append(f"          - name: {c.name}")
                lines.append(f"            type: {c.type}")
                lines.append(f"            nullable: {str(c.nullable).lower()}")
            if t.relationships:
                lines.append(f"        relationships:")
                for r in t.relationships:
                    lines.append(f"          - fk: {r.fk_table}.{r.fk_column}")
                    lines.append(f"            pk: {r.pk_table}.{r.pk_column}")
                    lines.append(f"            confidence: {r.confidence}")

    # Write out
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"✅ Wrote schema overview to {out_path}")


# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Export a text-based ERD for LLMs from a DuckDB warehouse.")
    ap.add_argument("--db", required=True, help="Path to DuckDB file, e.g., db/warehouse.duckdb")
    ap.add_argument("--out", default="schema_overview.txt", help="Output .txt path (YAML-like)")
    args = ap.parse_args()
    export_schema(args.db, args.out)
