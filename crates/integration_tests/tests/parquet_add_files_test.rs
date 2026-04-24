// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for `ParquetWriter::parquet_files_to_data_files` —
//! the add-files path against a real REST catalog + S3-compatible
//! storage (via `dev/docker-compose.yaml`).

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field};
use common::random_ns;
use iceberg::spec::{
    DataFileFormat, NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type,
    UnboundPartitionField,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriter, ParquetWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

async fn rest_catalog() -> impl Catalog {
    let fixture = get_test_fixture();
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap()
}

/// Single-column `id: long` schema — simplest shape that still exercises
/// partition-on-source-column inference.
fn id_long_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap()
}

fn identity_on_id_spec(schema: &Schema) -> PartitionSpec {
    PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .add_unbound_fields(vec![
            UnboundPartitionField::builder()
                .source_id(1)
                .name("id".to_string())
                .transform(Transform::Identity)
                .build(),
        ])
        .unwrap()
        .build()
        .unwrap()
}

fn bucket_on_id_spec(schema: &Schema) -> PartitionSpec {
    PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .add_unbound_fields(vec![
            UnboundPartitionField::builder()
                .source_id(1)
                .name("id_bucket".to_string())
                .transform(Transform::Bucket(16))
                .build(),
        ])
        .unwrap()
        .build()
        .unwrap()
}

/// Write a Parquet file containing a single `Int64` column `id` with the
/// given values into the table's data location. Returns the absolute S3
/// URI of the written file, suitable for feeding back into
/// `parquet_files_to_data_files`.
async fn write_id_parquet(table: &Table, values: Vec<i64>, tag: &str) -> String {
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_gen =
        DefaultFileNameGenerator::new(tag.to_string(), None, DataFileFormat::Parquet);
    let output_file = table
        .file_io()
        .new_output(location_gen.generate_location(None, &file_name_gen.generate_file_name()))
        .unwrap();
    let file_path = output_file.location().to_string();

    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
    ]));
    let col = Arc::new(Int64Array::from(values)) as ArrayRef;
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![col]).unwrap();

    let mut writer = ParquetWriterBuilder::new(
        WriterProperties::default(),
        Arc::new(arrow_schema.as_ref().try_into().unwrap()),
    )
    .build(output_file)
    .await
    .unwrap();
    writer.write(&batch).await.unwrap();
    let _ = writer.close().await.unwrap();

    file_path
}

async fn commit_fast_append(catalog: &impl Catalog, table: &Table, paths: Vec<String>) -> Table {
    let data_files =
        ParquetWriter::parquet_files_to_data_files(table.file_io(), paths, table.metadata())
            .await
            .unwrap();

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).unwrap();
    tx.commit(catalog as &dyn Catalog).await.unwrap()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_parquet_files_to_data_files_unpartitioned_happy() {
    let catalog = rest_catalog().await;
    let ns = random_ns().await;

    let table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t_unpart".to_string())
                .schema(id_long_schema())
                .build(),
        )
        .await
        .unwrap();

    // Two files, 16 rows each, same schema, no partition spec.
    let p1 = write_id_parquet(&table, (0..16).collect(), "a").await;
    let p2 = write_id_parquet(&table, (100..116).collect(), "b").await;

    let updated = commit_fast_append(&catalog, &table, vec![p1, p2]).await;
    let snap = updated.metadata().current_snapshot().unwrap();
    assert_eq!(
        snap.summary().additional_properties.get("total-records").map(String::as_str),
        Some("32"),
    );
    assert_eq!(
        snap.summary().additional_properties.get("added-data-files").map(String::as_str),
        Some("2"),
    );
}

#[tokio::test]
async fn test_parquet_files_to_data_files_identity_partition_happy() {
    let catalog = rest_catalog().await;
    let ns = random_ns().await;
    let schema = id_long_schema();

    let table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t_identity".to_string())
                .schema(schema.clone())
                .partition_spec(identity_on_id_spec(&schema))
                .build(),
        )
        .await
        .unwrap();

    // Two single-partition files: all rows in each have the same `id`.
    let p1 = write_id_parquet(&table, vec![42; 10], "p42").await;
    let p2 = write_id_parquet(&table, vec![99; 10], "p99").await;

    let updated = commit_fast_append(&catalog, &table, vec![p1, p2]).await;

    // The two DataFiles should land in distinct partitions.
    let manifest_list = updated
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(updated.file_io(), updated.metadata())
        .await
        .unwrap();

    let mut partitions = vec![];
    for entry in manifest_list.entries() {
        let manifest = entry.load_manifest(updated.file_io()).await.unwrap();
        for me in manifest.entries() {
            partitions.push(me.data_file().partition().clone());
        }
    }
    assert_eq!(partitions.len(), 2);
    // Two distinct partitions recovered from column bounds.
    let unique: std::collections::HashSet<_> = partitions.into_iter().collect();
    assert_eq!(unique.len(), 2);
}

#[tokio::test]
async fn test_parquet_files_to_data_files_cross_partition_rejects() {
    let catalog = rest_catalog().await;
    let ns = random_ns().await;
    let schema = id_long_schema();

    let table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t_cross".to_string())
                .schema(schema.clone())
                .partition_spec(identity_on_id_spec(&schema))
                .build(),
        )
        .await
        .unwrap();

    // File containing two distinct id values under an identity partition:
    // its rows span two partitions, which add_files must refuse.
    let bad = write_id_parquet(&table, vec![1, 1, 2, 2], "mixed").await;

    let err = ParquetWriter::parquet_files_to_data_files(
        table.file_io(),
        vec![bad],
        table.metadata(),
    )
    .await
    .expect_err("cross-partition file must error");

    assert_eq!(err.kind(), ErrorKind::DataInvalid);
    assert!(
        err.message().contains("more than one partition values"),
        "unexpected message: {}",
        err.message()
    );
}

#[tokio::test]
async fn test_parquet_files_to_data_files_bucket_transform_unsupported() {
    let catalog = rest_catalog().await;
    let ns = random_ns().await;
    let schema = id_long_schema();

    let table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t_bucket".to_string())
                .schema(schema.clone())
                .partition_spec(bucket_on_id_spec(&schema))
                .build(),
        )
        .await
        .unwrap();

    let p = write_id_parquet(&table, vec![7; 8], "b").await;

    let err = ParquetWriter::parquet_files_to_data_files(
        table.file_io(),
        vec![p],
        table.metadata(),
    )
    .await
    .expect_err("bucket transform must error");

    assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
}
