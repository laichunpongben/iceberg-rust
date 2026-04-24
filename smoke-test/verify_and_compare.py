"""Verify the Rust-registered smoke-test table and diff DataFile records
against the pyiceberg-registered reference.

Success criteria:
- Both tables expose exactly 6 DataFiles (matches setup_fixture.py plan).
- For each ordered (event_month, symbol) partition, the two tables'
  DataFiles agree on record_count, null_value_counts, value_counts,
  lower_bounds, and upper_bounds (file_path differs by table prefix).

Exit 0 on match, 1 on any divergence.
"""
from __future__ import annotations

import sys
from collections import defaultdict

from pyiceberg.catalog import load_catalog

REST_URI = "http://localhost:8181"
S3_ENDPOINT = "http://localhost:9000"
S3_REGION = "us-east-1"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"
WAREHOUSE = "s3://icebergdata/smoke-test"

CATALOG_PROPS = {
    "type": "rest",
    "uri": REST_URI,
    "warehouse": WAREHOUSE,
    "s3.endpoint": S3_ENDPOINT,
    "s3.region": S3_REGION,
    "s3.access-key-id": S3_ACCESS_KEY,
    "s3.secret-access-key": S3_SECRET_KEY,
    "s3.path-style-access": "true",
}


def _datafile_fingerprint(df):
    """Return a comparable dict of the DataFile fields that matter for
    equivalence, stripped of table-prefix-specific bits (file_path)."""
    return {
        "record_count": df.record_count,
        "file_format": str(df.file_format),
        "partition": tuple(df.partition),
        "value_counts": dict(df.value_counts or {}),
        "null_value_counts": dict(df.null_value_counts or {}),
        "lower_bounds": dict(df.lower_bounds or {}),
        "upper_bounds": dict(df.upper_bounds or {}),
    }


def _fingerprints_by_filename(tbl):
    """Return {basename: fingerprint} — two tables register the same
    files under different prefixes, so match them by filename."""
    out: dict[str, dict] = {}
    for task in tbl.scan().plan_files():
        df = task.file
        name = df.file_path.rsplit("/", 1)[-1]
        out[name] = _datafile_fingerprint(df)
    return out


def main() -> int:
    catalog = load_catalog("local", **CATALOG_PROPS)
    rust_tbl = catalog.load_table("ns.rust_test.l2")
    py_tbl = catalog.load_table("ns.rust_test.l2_via_pyiceberg")

    rust_fp = _fingerprints_by_filename(rust_tbl)
    py_fp = _fingerprints_by_filename(py_tbl)

    print(f"rust:      {len(rust_fp)} data files")
    print(f"pyiceberg: {len(py_fp)} data files")

    if set(rust_fp) != set(py_fp):
        print("FILENAME SET MISMATCH")
        print(f"  only in rust:      {sorted(set(rust_fp) - set(py_fp))}")
        print(f"  only in pyiceberg: {sorted(set(py_fp) - set(rust_fp))}")
        return 1

    any_diff = False
    for name in sorted(rust_fp):
        r, p = rust_fp[name], py_fp[name]
        if r != p:
            any_diff = True
            print(f"MISMATCH on {name}:")
            for k in sorted(set(r) | set(p)):
                if r.get(k) != p.get(k):
                    print(f"  {k}: rust={r.get(k)} pyiceberg={p.get(k)}")
        else:
            print(f"OK  {name} partition={r['partition']} record_count={r['record_count']}")

    return 1 if any_diff else 0


if __name__ == "__main__":
    sys.exit(main())
