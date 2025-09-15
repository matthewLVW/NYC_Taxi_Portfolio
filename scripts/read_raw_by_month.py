from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# This script downloads monthly NYC TLC Parquet files into data/raw.
# Default range: 2024-01 through 2025-04, service: yellow


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{yyyy}-{mm}.parquet"


def month_range(start: dt.date, end: dt.date) -> Iterator[dt.date]:
    """Yield first-of-month dates from start to end inclusive."""
    if start > end:
        return
    year, month = start.year, start.month
    while True:
        current = dt.date(year, month, 1)
        if current > end:
            break
        yield current
        # increment month
        month += 1
        if month > 12:
            month = 1
            year += 1


def build_url(service: str, d: dt.date) -> str:
    return BASE_URL.format(service=service, yyyy=d.year, mm=f"{d.month:02d}")


def download(url: str, dest: Path, timeout: int = 60) -> None:
    """Stream download to a temp file then atomically move into place."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest.with_suffix(dest.suffix + ".part")
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; downloader/1.0)"})
    try:
        with urlopen(req, timeout=timeout) as resp, open(tmp_path, "wb") as f:
            chunk = resp.read(1024 * 1024)
            total = 0
            while chunk:
                f.write(chunk)
                total += len(chunk)
                chunk = resp.read(1024 * 1024)
        # Basic sanity check
        if tmp_path.stat().st_size < 1024 * 10:  # <10KB is suspicious
            raise RuntimeError(f"Downloaded file too small: {tmp_path}")
        os.replace(tmp_path, dest)
    finally:
        # Clean up temp on failure
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Download NYC TLC monthly Parquet files into data/raw")
    parser.add_argument("--service", default="yellow", choices=["yellow", "green", "fhv"], help="Dataset service type")
    parser.add_argument("--start", default="2024-01", help="Start month YYYY-MM (inclusive)")
    parser.add_argument("--end", default="2025-04", help="End month YYYY-MM (inclusive)")
    parser.add_argument("--raw-dir", default="data/raw", help="Destination directory")
    parser.add_argument("--overwrite", action="store_true", help="Re-download even if file exists")
    parser.add_argument("--dry-run", action="store_true", help="List work without downloading")

    args = parser.parse_args()

    try:
        start = dt.date.fromisoformat(args.start + "-01")
        end = dt.date.fromisoformat(args.end + "-01")
    except ValueError as e:
        print(f"Invalid date format: {e}")
        return 2

    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    service = args.service
    to_get = list(month_range(start, end))
    if not to_get:
        print("No months to process.")
        return 0

    successes = 0
    skips = 0
    failures = 0

    for d in to_get:
        fname = f"{service}_tripdata_{d.year}-{d.month:02d}.parquet"
        dest = raw_dir / fname
        url = build_url(service, d)

        if dest.exists() and not args.overwrite:
            print(f"Skip (exists): {dest}")
            skips += 1
            continue

        if args.dry_run:
            print(f"Would download: {url} -> {dest}")
            continue

        print(f"Downloading {url} -> {dest}")
        try:
            download(url, dest)
            print(f"OK: {dest} ({dest.stat().st_size/1_000_000:.1f} MB)")
            successes += 1
        except HTTPError as e:
            print(f"HTTP error for {url}: {e}")
            failures += 1
        except URLError as e:
            print(f"URL error for {url}: {e}")
            failures += 1
        except Exception as e:
            print(f"Failed {url}: {e}")
            failures += 1

    print(f"Done. ok={successes} skip={skips} fail={failures}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
