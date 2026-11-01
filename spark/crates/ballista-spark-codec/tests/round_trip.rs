//! Round-trip tests: plans encoded with [`SparkPhysicalCodec`] decode back to
//! the same plan, whichever upstream codec owns each node.

use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_spark_codec::SparkPhysicalCodec;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
};
use sail_logical_plan::range::Range;
use sail_physical_plan::range::RangeExec;

fn sail_range(partitions: usize) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    Ok(Arc::new(RangeExec::try_new(
        Range {
            start: 0,
            end: 100,
            step: 1,
        },
        partitions,
        schema,
        vec![0],
    )?))
}

fn round_trip(plan: Arc<dyn ExecutionPlan>) -> Result<(String, String)> {
    let codec = SparkPhysicalCodec::new();
    let bytes = physical_plan_to_bytes_with_extension_codec(plan.clone(), &codec)?;
    let ctx = SessionContext::new();
    let decoded = physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), &codec)?;
    Ok((
        displayable(plan.as_ref()).indent(true).to_string(),
        displayable(decoded.as_ref()).indent(true).to_string(),
    ))
}

#[test]
fn sail_node_round_trips() -> Result<()> {
    let (before, after) = round_trip(sail_range(4)?)?;
    assert_eq!(before, after);
    assert!(after.contains("RangeExec"), "{after}");
    Ok(())
}

#[test]
fn ballista_node_over_sail_node_round_trips() -> Result<()> {
    let writer = ShuffleWriterExec::try_new(
        JobId::new("job-1"),
        1,
        sail_range(4)?,
        "/tmp/ballista-spark-codec-test".to_string(),
    )?;
    let (before, after) = round_trip(Arc::new(writer))?;
    assert_eq!(before, after);
    assert!(after.contains("ShuffleWriterExec"), "{after}");
    assert!(after.contains("RangeExec"), "{after}");
    Ok(())
}

#[test]
fn unknown_origin_tag_is_rejected() {
    use datafusion_proto::physical_plan::{DefaultPhysicalProtoConverter, PhysicalExtensionCodec};
    let codec = SparkPhysicalCodec::new();
    let ctx = SessionContext::new();
    let err = codec
        .try_decode(&[42, 1, 2, 3], &[], &ctx.task_ctx(), &DefaultPhysicalProtoConverter {})
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("unknown extension plan node origin tag 42"), "{err}");
}

#[test]
fn empty_payload_is_rejected() {
    use datafusion_proto::physical_plan::{DefaultPhysicalProtoConverter, PhysicalExtensionCodec};
    let codec = SparkPhysicalCodec::new();
    let ctx = SessionContext::new();
    let err = codec
        .try_decode(&[], &[], &ctx.task_ctx(), &DefaultPhysicalProtoConverter {})
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("empty extension plan node"), "{err}");
}

/// A Sail node whose child is a Ballista node: Sail's codec embeds its children,
/// so it must encode them with the composite codec, not with itself.
#[test]
fn sail_node_over_ballista_node_round_trips() -> Result<()> {
    use ballista_core::execution_plans::SelectPartitionsExec;
    use sail_physical_plan::coalesce::CoalesceExec;

    let select = SelectPartitionsExec::try_new(sail_range(8)?, vec![4, 5, 6, 7])?;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalesceExec::new(Arc::new(select), 2));
    let codec = SparkPhysicalCodec::shared();
    let bytes = physical_plan_to_bytes_with_extension_codec(plan.clone(), codec.as_ref())?;
    let ctx = SessionContext::new();
    let decoded =
        physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), codec.as_ref())?;
    let before = displayable(plan.as_ref()).indent(true).to_string();
    let after = displayable(decoded.as_ref()).indent(true).to_string();
    assert_eq!(before, after);
    assert!(after.contains("SelectPartitionsExec"), "{after}");
    Ok(())
}

/// A Parquet scan that projects a reader-produced virtual column (the row
/// number Iceberg row-level operations read) keeps that column after a round
/// trip, so projections over it still resolve (Iceberg `UPDATE` / `MERGE`).
#[test]
fn parquet_scan_with_virtual_column_round_trips() -> Result<()> {
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use parquet::arrow::RowNumber;

    let file_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
    ]));
    let partition_cols = vec![
        Arc::new(Field::new("p1", DataType::Utf8, false)),
        Arc::new(Field::new("p2", DataType::Int32, false)),
        Arc::new(Field::new("p3", DataType::Utf8, false)),
    ];
    let row_number = Arc::new(
        Field::new("row_position", DataType::Int64, false).with_extension_type(RowNumber),
    );
    let table_schema = TableSchema::builder(file_schema)
        .with_table_partition_cols(partition_cols)
        .with_virtual_columns(vec![row_number])
        .build();
    let source = Arc::new(ParquetSource::new(table_schema));
    let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
        .with_file(PartitionedFile::new("data/k=3/part-0.parquet", 100))
        .with_projection_indices(Some(vec![0, 1, 2, 3, 4, 5]))?
        .build();
    let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
    let wrapped = ballista_core::execution_plans::CodecScanExec::wrap_scans(plan.clone())?;
    let codec = SparkPhysicalCodec::shared();
    let bytes = physical_plan_to_bytes_with_extension_codec(wrapped, codec.as_ref())?;
    let ctx = SessionContext::new();
    let decoded =
        physical_plan_from_bytes_with_extension_codec(&bytes, &ctx.task_ctx(), codec.as_ref())?;
    assert_eq!(plan.schema(), decoded.schema());
    let shown = displayable(decoded.as_ref()).indent(true).to_string();
    assert!(shown.contains("row_position"), "{shown}");
    Ok(())
}
