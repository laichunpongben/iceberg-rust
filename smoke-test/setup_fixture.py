"""Set up the smoke-test fixtures:

1. Create namespace `ns.rust_test` in the local REST catalog.
2. Create two identical tables: `l2` (target of Rust register) and
   `l2_via_pyiceberg` (reference register via pyiceberg).
3. Generate a handful of small synthetic parquets spanning multiple
   (trading_day, symbol) partitions and upload them into MinIO under
   paths the target table expects.

Expected schema: NSE-flavored L2 with identity partitions on `event_month`
and `symbol`, vendor field IDs 10001+ to exercise the same code path the
real register uses.

Usage (from smoke-test/):
    uv run --with "pyiceberg[s3fs]" --with pyarrow setup_fixture.py
"""
from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# ---------------------------------------------------------------------------
# Endpoints (see README.md — the REST catalog points at http://minio:9000
# which is the Docker alias; we override to localhost for host-side clients).
# ---------------------------------------------------------------------------

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

NAMESPACE = ("ns", "rust_test")
RUST_TABLE = (*NAMESPACE, "l2")
PY_TABLE = (*NAMESPACE, "l2_via_pyiceberg")

# NSE-flavored schema: a subset of the real L2 schema plus a vendor
# field at id 10001 to make sure the binary doesn't renumber field ids
# under its feet.
SCHEMA = Schema(
    NestedField(1, "event_time", TimestamptzType(), required=True),
    NestedField(2, "symbol", StringType(), required=True),
    NestedField(3, "bid_px", LongType(), required=False),
    NestedField(4, "ask_px", LongType(), required=False),
    NestedField(10001, "_vendor_seq", LongType(), required=False),
)

SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=MonthTransform(), name="event_month"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="symbol"),
)


def _parquet_schema_for_iceberg() -> pa.Schema:
    """Arrow schema with Parquet field-id metadata that round-trips back
    to the Iceberg schema above."""
    fields = [
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False,
                metadata={b"PARQUET:field_id": b"1"}),
        pa.field("symbol", pa.string(), nullable=False,
                metadata={b"PARQUET:field_id": b"2"}),
        pa.field("bid_px", pa.int64(), nullable=True,
                metadata={b"PARQUET:field_id": b"3"}),
        pa.field("ask_px", pa.int64(), nullable=True,
                metadata={b"PARQUET:field_id": b"4"}),
        pa.field("_vendor_seq", pa.int64(), nullable=True,
                metadata={b"PARQUET:field_id": b"10001"}),
    ]
    return pa.schema(fields)


def _synthetic_batch(symbol: str, day: str, rows: int = 32) -> pa.Table:
    """Tiny synthetic batch, all rows within one UTC day so partition
    inference has a single (event_month, symbol) answer per file."""
    base = datetime.fromisoformat(f"{day}T03:45:00+00:00")  # ~09:15 IST, post-open
    timestamps = [base.replace(second=i % 60, microsecond=(i * 1000) % 1_000_000)
                  for i in range(rows)]
    return pa.Table.from_pydict(
        {
            "event_time": timestamps,
            "symbol": [symbol] * rows,
            "bid_px": list(range(10000, 10000 + rows)),
            "ask_px": list(range(10001, 10001 + rows)),
            "_vendor_seq": list(range(rows)),
        },
        schema=_parquet_schema_for_iceberg(),
    )


def _upload_parquets_to_s3(catalog, tmpdir: Path, data_prefix: str) -> list[str]:
    """Write a handful of parquets locally, then upload them via the
    same S3FileIO pyiceberg uses, so auth/endpoint stay consistent."""
    # Spread across 3 symbols × 2 dates = 6 files, single-partition each.
    plan = [
        ("ACME", "2026-02-02"),
        ("ACME", "2026-02-03"),
        ("FOO", "2026-02-02"),
        ("FOO", "2026-02-03"),
        ("BAR-WIDGET", "2026-02-02"),  # hyphenated symbol exercises escaping
        ("BAR-WIDGET", "2026-02-03"),
    ]
    s3_paths: list[str] = []
    io = catalog._fs  # internal but stable; we need the io used by pyiceberg
    for i, (symbol, day) in enumerate(plan):
        table = _synthetic_batch(symbol, day)
        local_path = tmpdir / f"{i:02d}_{symbol}_{day}.parquet"
        pq.write_table(table, local_path, compression="snappy")
        s3_key = f"{data_prefix}/{day}/{symbol}/{local_path.name}"
        with open(local_path, "rb") as f, io.open(s3_key, "wb") as out:
            out.write(f.read())
        s3_paths.append(s3_key)
    return s3_paths


def main() -> None:
    catalog = load_catalog("local", **CATALOG_PROPS)
    _setup_namespace_and_tables(catalog)

    out_dir = Path(__file__).parent
    paths_file = out_dir / "paths.txt"

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        rust_prefix = f"{WAREHOUSE}/rust-test-data"
        py_prefix = f"{WAREHOUSE}/py-test-data"

        # Write + upload one set of source parquets, then server-copy
        # them to the pyiceberg-side prefix as well so both tables
        # reference disjoint paths but identical contents.
        from pyiceberg.io.pyarrow import PyArrowFileIO

        fs_props = {
            "s3.endpoint": S3_ENDPOINT,
            "s3.region": S3_REGION,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.path-style-access": "true",
        }
        io = PyArrowFileIO(fs_props)

        rust_paths: list[str] = []
        py_paths: list[str] = []
        for i, (symbol, day) in enumerate(_plan()):
            table = _synthetic_batch(symbol, day)
            local_path = tmpdir / f"{i:02d}_{symbol}_{day}.parquet"
            pq.write_table(table, local_path, compression="snappy")

            for prefix, target in ((rust_prefix, rust_paths), (py_prefix, py_paths)):
                key = f"{prefix}/{day}/{symbol}/{local_path.name}"
                with open(local_path, "rb") as f, io.new_output(key).create(overwrite=True) as out:
                    out.write(f.read())
                target.append(key)

        paths_file.write_text("\n".join(rust_paths) + "\n")
        print(f"uploaded {len(rust_paths)} parquets for rust target")
        print(f"uploaded {len(py_paths)} parquets for pyiceberg reference")
        print(f"wrote rust paths to {paths_file}")

        # Register the reference set via pyiceberg now; the Rust set is
        # registered later by the binary.
        ref_tbl = catalog.load_table(".".join(PY_TABLE))
        ref_tbl.add_files(file_paths=py_paths, check_duplicate_files=False)
        ref_snap = list(ref_tbl.snapshots())[-1]
        print(f"pyiceberg reference committed: snapshots=1, "
              f"total-records={ref_snap.summary.get('total-records')}")


def _plan() -> list[tuple[str, str]]:
    return [
        ("ACME", "2026-02-02"),
        ("ACME", "2026-02-03"),
        ("FOO", "2026-02-02"),
        ("FOO", "2026-02-03"),
        ("BAR-WIDGET", "2026-02-02"),
        ("BAR-WIDGET", "2026-02-03"),
    ]


def _setup_namespace_and_tables(catalog) -> None:
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

    # Fresh namespace each run. Drop-then-create so a rerun is idempotent.
    for tbl in (RUST_TABLE, PY_TABLE):
        try:
            catalog.drop_table(".".join(tbl))
            print(f"dropped {'.'.join(tbl)}")
        except NoSuchTableError:
            pass

    try:
        catalog.drop_namespace(NAMESPACE)
        print(f"dropped namespace {'.'.join(NAMESPACE)}")
    except NoSuchNamespaceError:
        pass

    catalog.create_namespace(NAMESPACE)
    print(f"created namespace {'.'.join(NAMESPACE)}")

    for tbl in (RUST_TABLE, PY_TABLE):
        catalog.create_table(
            identifier=".".join(tbl),
            schema=SCHEMA,
            partition_spec=SPEC,
            properties={"write.parquet.compression-codec": "snappy"},
        )
        print(f"created {'.'.join(tbl)}")


if __name__ == "__main__":
    main()
