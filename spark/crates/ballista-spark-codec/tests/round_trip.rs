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
