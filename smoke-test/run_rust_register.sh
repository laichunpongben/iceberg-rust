#!/usr/bin/env bash
# Run the Rust binary against the smoke-test target table with the
# paths file produced by setup_fixture.py.
set -euo pipefail

cd "$(dirname "$0")"

BIN="../target/release/iceberg-register"
if [[ ! -x "$BIN" ]]; then
    echo "building $BIN (release)..."
    (cd .. && cargo build -p iceberg-register --release)
fi

if [[ ! -f paths.txt ]]; then
    echo "paths.txt missing — run setup_fixture.py first" >&2
    exit 1
fi

AWS_ACCESS_KEY_ID=admin \
AWS_SECRET_ACCESS_KEY=password \
"$BIN" \
    --catalog-uri    http://localhost:8181 \
    --warehouse      s3://icebergdata \
    --s3-endpoint    http://localhost:9000 \
    --s3-region      us-east-1 \
    --s3-path-style  true \
    --table-id       ns.rust_test.l2 \
    --paths-file     ./paths.txt
