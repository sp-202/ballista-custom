//! A physical plan codec for plans that mix Ballista and Sail operators.
//!
//! Spark queries are planned by Sail and executed by Ballista, so one plan can
//! contain Ballista's distributed operators (shuffle writers/readers, added by
//! the scheduler) and Sail's operators and Spark functions (added by the
//! Spark planner). Neither upstream codec handles the other's nodes, and
//! Ballista's codec does not fall back to another codec for unknown nodes.
//!
//! [`SparkPhysicalCodec`] routes each call to the codec that owns the type:
//!
//! - **Plan nodes**: tried with Ballista first, then Sail. The encoded bytes
//!   are prefixed with a one-byte [`Origin`] tag so decoding never has to guess.
//! - **Functions and expressions** (UDF, UDAF, UDWF, higher-order functions,
//!   custom physical expressions): Ballista defines none, so these go to Sail.
//!   Sail encodes its own functions with a payload and leaves built-in
//!   DataFusion functions empty, which DataFusion then resolves by name.
//!
//! Ballista nodes that embed expressions (e.g. the hash-partitioning
//! expressions of a shuffle writer) encode them with Sail's codec too, via
//! `BallistaPhysicalExtensionCodec::new`, so Spark functions can appear there.

use std::sync::Arc;

use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion::physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use sail_execution::RemoteExecutionCodec;

/// Which codec produced an encoded plan node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Origin {
    Ballista = 1,
    Sail = 2,
}

impl Origin {
    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Ballista),
            2 => Some(Self::Sail),
            _ => None,
        }
    }
}

/// See the [crate documentation](crate).
#[derive(Debug)]
pub struct SparkPhysicalCodec {
    ballista: BallistaPhysicalExtensionCodec,
    sail: Arc<RemoteExecutionCodec>,
}

impl Default for SparkPhysicalCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkPhysicalCodec {
    pub fn new() -> Self {
        Self::with_sail(Arc::new(RemoteExecutionCodec::new()))
    }

    fn with_sail(sail: Arc<RemoteExecutionCodec>) -> Self {
        Self {
            ballista: BallistaPhysicalExtensionCodec::new(sail.clone()),
            sail,
        }
    }

    /// The codec as the trait object Ballista's configs expect. Plans nested in
    /// Sail's nodes are encoded with this composite codec too, so they may hold
    /// Ballista nodes (a shuffle reader under `CoalesceExec`).
    pub fn shared() -> Arc<dyn PhysicalExtensionCodec> {
        Arc::<Self>::new_cyclic(|composite| {
            let nested: std::sync::Weak<dyn PhysicalExtensionCodec> = composite.clone();
            Self::with_sail(Arc::new(RemoteExecutionCodec::with_nested_codec(nested)))
        })
    }

    fn codec(&self, origin: Origin) -> &dyn PhysicalExtensionCodec {
        match origin {
            Origin::Ballista => &self.ballista,
            Origin::Sail => self.sail.as_ref(),
        }
    }

    /// Encodes with `origin`'s codec into `buf` after the tag byte. On failure
    /// `buf` is restored, so the next codec starts from a clean buffer.
    fn try_encode_with(
        &self,
        origin: Origin,
        node: &Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let start = buf.len();
        buf.push(origin as u8);
        let result = self
            .codec(origin)
            .try_encode(node.clone(), buf, proto_converter);
        if result.is_err() {
            buf.truncate(start);
        }
        result
    }
}

impl PhysicalExtensionCodec for SparkPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some((&tag, payload)) = buf.split_first() else {
            return internal_err!("empty extension plan node");
        };
        let Some(origin) = Origin::from_tag(tag) else {
            return internal_err!("unknown extension plan node origin tag {tag}");
        };
        self.codec(origin)
            .try_decode(payload, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        match self.try_encode_with(Origin::Ballista, &node, buf, proto_converter) {
            Ok(()) => Ok(()),
            Err(ballista_err) => self
                .try_encode_with(Origin::Sail, &node, buf, proto_converter)
                .map_err(|sail_err| {
                    datafusion::common::DataFusionError::Internal(format!(
                        "no codec can encode plan node {}: ballista: {ballista_err}; sail: {sail_err}",
                        node.name()
                    ))
                }),
        }
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.sail.try_decode_udf(name, buf)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.sail.try_encode_udf(node, buf)
    }

    fn try_decode_higher_order_function(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<HigherOrderUDF>> {
        self.sail.try_decode_higher_order_function(name, buf)
    }

    fn try_encode_higher_order_function(
        &self,
        node: &HigherOrderUDF,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        self.sail.try_encode_higher_order_function(node, buf)
    }

    fn try_decode_expr(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn PhysicalExpr>],
        ctx: &PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.sail.try_decode_expr(buf, inputs, ctx)
    }

    fn try_encode_expr(
        &self,
        node: &Arc<dyn PhysicalExpr>,
        buf: &mut Vec<u8>,
        ctx: &PhysicalExprEncodeCtx<'_>,
    ) -> Result<()> {
        self.sail.try_encode_expr(node, buf, ctx)
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.sail.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.sail.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        self.sail.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.sail.try_encode_udwf(node, buf)
    }
}
