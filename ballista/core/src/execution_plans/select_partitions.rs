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

//! Exposes a chosen subset of a leaf's partitions.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

/// Output partition `i` is partition `partitions[i]` of the input.
///
/// Task plans use it to restrict a leaf the scheduler has no specific rule for
/// (for example Spark's `RangeExec`) to the task's partitions. Without it every
/// task of the stage would run all of the leaf's partitions. A leaf's partitions
/// are independent of each other, so any leaf can be restricted this way. Like
/// `EmptyPartitionsExec` it reports no metrics, so a task plan has the same
/// metrics slots as its stage plan.
#[derive(Debug, Clone)]
pub struct SelectPartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    partitions: Vec<usize>,
    properties: Arc<PlanProperties>,
}

impl SelectPartitionsExec {
    /// Selects `partitions` (indices into `input`'s partitions) from `input`.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitions: Vec<usize>,
    ) -> Result<Self> {
        let count = input.properties().partitioning.partition_count();
        if let Some(bad) = partitions.iter().find(|&&p| p >= count) {
            return Err(DataFusionError::Internal(format!(
                "SelectPartitionsExec: partition {bad} out of range, {} has {count}",
                input.name()
            )));
        }
        let partitioning = match &input.properties().partitioning {
            Partitioning::Hash(exprs, _) => {
                Partitioning::Hash(exprs.clone(), partitions.len())
            }
            _ => Partitioning::UnknownPartitioning(partitions.len()),
        };
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_partitioning(partitioning),
        );
        Ok(Self {
            input,
            partitions,
            properties,
        })
    }

    /// The selected input partitions, in output order.
    pub fn partitions(&self) -> &[usize] {
        &self.partitions
    }
}

impl DisplayAs for SelectPartitionsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SelectPartitionsExec: partitions={:?}", self.partitions)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitions={:?}", self.partitions)
            }
        }
    }
}

impl ExecutionPlan for SelectPartitionsExec {
    fn name(&self) -> &str {
        "SelectPartitionsExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.as_slice() {
            [input] => Ok(Arc::new(Self::try_new(
                Arc::clone(input),
                self.partitions.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(format!(
                "SelectPartitionsExec expects one child, got {}",
                children.len()
            ))),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_partition = *self.partitions.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "SelectPartitionsExec has {} partitions, asked for partition {partition}",
                self.partitions.len()
            ))
        })?;
        self.input.execute(input_partition, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::common::collect;

    fn batch(schema: &SchemaRef, v: i64) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![v]))])
            .unwrap()
    }

    #[tokio::test]
    async fn exposes_the_selected_partitions_in_order() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let parts: Vec<Vec<RecordBatch>> =
            (0..4).map(|i| vec![batch(&schema, i)]).collect();
        let input = MemorySourceConfig::try_new_exec(&parts, schema, None)?;
        let select = SelectPartitionsExec::try_new(input, vec![3, 1])?;
        assert_eq!(select.properties().partitioning.partition_count(), 2);
        let ctx = Arc::new(TaskContext::default());
        let mut values = vec![];
        for p in 0..2 {
            for b in collect(select.execute(p, ctx.clone())?).await? {
                values.push(
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                );
            }
        }
        assert_eq!(values, vec![3, 1]);
        assert!(select.execute(2, ctx).is_err());
        Ok(())
    }

    #[test]
    fn rejects_out_of_range_partitions() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let input =
            MemorySourceConfig::try_new_exec(&[vec![], vec![]], schema, None).unwrap();
        assert!(SelectPartitionsExec::try_new(input, vec![2]).is_err());
    }
}
