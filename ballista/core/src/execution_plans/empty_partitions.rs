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

//! A leaf with a given number of empty partitions that serializes its partition count.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

/// Produces no rows from `partitions` partitions, like DataFusion's `EmptyExec`.
///
/// Task plans use it in place of an `EmptyExec` or `PlaceholderRowExec` restricted to
/// zero partitions: DataFusion's protobuf drops those operators' partition count, so a
/// restriction to zero partitions would arrive as one partition. Like them, it reports
/// no metrics, so a task plan has the same metrics slots as its stage plan.
#[derive(Debug, Clone)]
pub struct EmptyPartitionsExec {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl EmptyPartitionsExec {
    /// A leaf with `partitions` empty partitions of `schema`.
    pub fn new(schema: SchemaRef, partitions: usize) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { schema, properties }
    }

    /// The number of (empty) output partitions.
    pub fn partitions(&self) -> usize {
        self.properties.partitioning.partition_count()
    }
}

impl DisplayAs for EmptyPartitionsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "EmptyPartitionsExec: partitions={}", self.partitions())
            }
            DisplayFormatType::TreeRender => {
                write!(f, "partitions={}", self.partitions())
            }
        }
    }
}

impl ExecutionPlan for EmptyPartitionsExec {
    fn name(&self) -> &str {
        "EmptyPartitionsExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
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
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "EmptyPartitionsExec does not support children plans".to_owned(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.partitions() {
            return Err(DataFusionError::Internal(format!(
                "EmptyPartitionsExec has {} partitions, asked for partition {partition}",
                self.partitions()
            )));
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::empty(),
        )))
    }
}
