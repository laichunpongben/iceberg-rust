//! `iceberg-register` — thin CLI over `iceberg-rust` to register existing
//! Parquet files with a REST-catalog-backed Iceberg table.
//!
//! Fork-only. Wraps `ParquetWriter::parquet_files_to_data_files` and
//! `FastAppendAction` in a shell-script-friendly binary, so a Python /
//! SLURM pipeline can drive register as a subprocess rather than via a
//! library dependency.
//!
//! Example:
//!
//! ```bash
//! iceberg-register \
//!   --catalog-uri http://nessie.infra:port/iceberg/main \
//!   --warehouse   s3://my-bucket \
//!   --s3-endpoint https://s3.eu-central-1.amazonaws.com \
//!   --s3-region   eu-central-1 \
//!   --table-id    my.namespace.nested.table \
//!   --paths-file  /tmp/paths.txt
//! ```
//!
//! `paths.txt` contains one S3 parquet URI per line, e.g.
//! `s3://bucket/path/to/data/2026-02-01/symbol/xxx.parquet`.
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::ParquetWriter;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

#[derive(Parser, Debug)]
#[command(
    name = "iceberg-register",
    about = "Register existing Parquet files with an Iceberg REST catalog"
)]
struct Cli {
    /// REST catalog URI (e.g. Nessie iceberg endpoint).
    #[arg(long)]
    catalog_uri: String,

    /// Warehouse URI (e.g. `s3://bucket`).
    #[arg(long)]
    warehouse: String,

    /// S3 endpoint. Required for non-AWS (MinIO, ceph) and AWS
    /// region-specific URLs.
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 region.
    #[arg(long)]
    s3_region: Option<String>,

    /// S3 access key ID. If unset, OpenDAL's default credential chain
    /// reads `AWS_ACCESS_KEY_ID` from the environment. Required for
    /// MinIO and non-instance-profile AWS.
    #[arg(long)]
    s3_access_key_id: Option<String>,

    /// S3 secret access key. If unset, falls back to the env var
    /// `AWS_SECRET_ACCESS_KEY` via OpenDAL's default chain.
    #[arg(long)]
    s3_secret_access_key: Option<String>,

    /// S3 path-style access. Default true (required for MinIO and most
    /// on-prem catalogs); pass `--s3-path-style=false` for AWS-hosted
    /// endpoints that require virtual-hosted-style URLs.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    s3_path_style: bool,

    /// Dotted table identifier, e.g. `ns1.ns2.table_name`.
    #[arg(long)]
    table_id: String,

    /// File containing one Parquet path per line (e.g.
    /// `s3://bucket/key.parquet`). Blank lines and lines starting
    /// with `#` are ignored.
    #[arg(long)]
    paths_file: PathBuf,

    /// Skip the `FastAppendAction` duplicate-file check. Use when
    /// you're certain paths aren't already registered and want to
    /// save the O(N) lookup. Default false (safe).
    #[arg(long)]
    skip_duplicate_check: bool,
}

fn parse_table_id(s: &str) -> Result<TableIdent> {
    TableIdent::from_strs(s.split('.')).context("invalid --table-id")
}

async fn load_paths(path: &PathBuf) -> Result<Vec<String>> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("reading --paths-file {}", path.display()))?;
    let paths: Vec<String> = raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
        .collect();
    if paths.is_empty() {
        bail!("--paths-file {} contained no paths", path.display());
    }
    Ok(paths)
}

async fn build_catalog(cli: &Cli) -> Result<impl Catalog> {
    let mut props = vec![
        (REST_CATALOG_PROP_URI.to_string(), cli.catalog_uri.clone()),
        (
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            cli.warehouse.clone(),
        ),
    ];
    if let Some(endpoint) = &cli.s3_endpoint {
        props.push((S3_ENDPOINT.to_string(), endpoint.clone()));
    }
    if let Some(region) = &cli.s3_region {
        props.push((S3_REGION.to_string(), region.clone()));
    }
    if let Some(key) = &cli.s3_access_key_id {
        props.push((S3_ACCESS_KEY_ID.to_string(), key.clone()));
    }
    if let Some(secret) = &cli.s3_secret_access_key {
        props.push((S3_SECRET_ACCESS_KEY.to_string(), secret.clone()));
    }
    props.push((
        S3_PATH_STYLE_ACCESS.to_string(),
        cli.s3_path_style.to_string(),
    ));

    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load("rest", props.into_iter().collect())
        .await
        .context("building REST catalog")
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let table_ident = parse_table_id(&cli.table_id)?;
    let paths = load_paths(&cli.paths_file).await?;
    println!("loaded {} paths from {}", paths.len(), cli.paths_file.display());

    let catalog = build_catalog(&cli).await?;
    let table = catalog
        .load_table(&table_ident)
        .await
        .with_context(|| format!("loading table {table_ident}"))?;
    println!(
        "loaded table {}: current snapshot = {:?}",
        table_ident,
        table.metadata().current_snapshot_id(),
    );

    let t0 = Instant::now();
    let data_files =
        ParquetWriter::parquet_files_to_data_files(table.file_io(), paths, table.metadata())
            .await
            .context("converting parquet files to data files")?;
    println!(
        "built {} DataFile records in {:?}",
        data_files.len(),
        t0.elapsed()
    );

    let t0 = Instant::now();
    let tx = Transaction::new(&table);
    let action = tx
        .fast_append()
        .with_check_duplicate(!cli.skip_duplicate_check)
        .add_data_files(data_files);
    let tx = action.apply(tx).context("applying FastAppendAction")?;
    let updated = tx.commit(&catalog).await.context("committing transaction")?;
    println!(
        "committed snapshot {:?} in {:?}",
        updated.metadata().current_snapshot_id(),
        t0.elapsed()
    );

    Ok(())
}
