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

//! Routes file scans to the extension codec when a plan is serialized.

use std::sync::Arc;

use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

/// A file scan to be serialized by the extension codec. Exists only while a
/// plan is encoded; decoding yields the scan itself.
///
/// DataFusion serializes `DataSourceExec` itself, before asking the extension
/// codec, and its encoding drops parts of a scan: reader-produced virtual
/// columns (e.g. the Parquet row number Iceberg row-level operations read) and
/// schema-evolution settings. Wrapping the scan sends it to the codec, which
/// can encode it completely (the Spark codec hands it to Sail); Ballista's own
/// codec stores DataFusion's encoding, so nothing changes without one.
#[derive(Debug)]
pub struct CodecScanExec {
    scan: Arc<dyn ExecutionPlan>,
}

impl CodecScanExec {
    /// The wrapped `DataSourceExec`.
    pub fn scan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.scan
    }

    /// Wraps every `DataSourceExec` in `plan`. Call it on a plan right before
    /// serializing it.
    pub fn wrap_scans(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if node.downcast_ref::<DataSourceExec>().is_some() {
                Ok(Transformed::yes(
                    Arc::new(CodecScanExec { scan: node }) as Arc<dyn ExecutionPlan>
                ))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .data()
    }
}

impl DisplayAs for CodecScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.scan.fmt_as(t, f)
    }
}

impl ExecutionPlan for CodecScanExec {
    fn name(&self) -> &str {
        "CodecScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.scan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.scan.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.scan.execute(partition, context)
    }
}
