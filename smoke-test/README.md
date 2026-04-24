# iceberg-register smoke test (Stage 1: local)

Validates the Rust `iceberg-register` binary end-to-end against
docker-composed MinIO + Apache iceberg-rest-fixture before trusting it
against real HPC Nessie.

## One-time setup

```bash
# From iceberg-rust repo root:
docker compose -f dev/docker-compose.yaml up -d --wait minio mc rest

# Verify:
curl -sf http://localhost:8181/v1/config | jq .
curl -sf http://localhost:9000/minio/health/live
```

The REST catalog advertises `http://minio:9000` as its S3 endpoint.
That's the Docker-network alias. From the host (this Mac / pyiceberg /
the Rust binary) we override the endpoint to `http://localhost:9000`.

## Steps

```bash
# 1. Set up test table + seed parquets in MinIO
uv run --with "pyiceberg[s3fs]" --with pyarrow setup_fixture.py

# 2. Run the Rust binary (builds on first invocation)
./run_rust_register.sh

# 3. Cross-check via pyiceberg (register same paths into a sibling table)
uv run --with "pyiceberg[s3fs]" --with pyarrow verify_and_compare.py
```

`verify_and_compare.py` exits 0 if the Rust-written and pyiceberg-written
tables have byte-identical `DataFile` records modulo table-prefix.

## Cleanup

```bash
docker compose -f ../dev/docker-compose.yaml down -v
```

## Namespaces

- `ns.rust_test.l2`             — written via Rust binary
- `ns.rust_test.l2_via_pyiceberg` — written via pyiceberg (reference)
