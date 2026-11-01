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

//! Checks that the file-based tables a logical plan reads exist on the scheduler.
//!
//! The scheduler re-plans a client's logical plan and lists table files itself. A
//! location the client can see but the scheduler cannot (a missing mount, a typo in
//! a shared path) lists as zero files, so the query reads an empty table and returns
//! a wrong answer without an error. Spark fails such a read with `PATH_NOT_FOUND`;
//! this does the same before the job is planned.

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::{ListingTable, ListingTableUrl};
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use futures::StreamExt;
use object_store::ObjectStoreExt;

/// Returns a `PATH_NOT_FOUND` plan error for the first listing-table location in
/// `plan` (including subqueries) that does not exist.
pub(crate) async fn check_table_paths_exist(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> Result<()> {
    let mut urls = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Ok(provider) = source_as_provider(&scan.source)
            && let Some(table) = provider.downcast_ref::<ListingTable>()
        {
            urls.extend(table.table_paths().iter().cloned());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    for url in urls {
        if !path_exists(ctx, &url).await? {
            return Err(DataFusionError::Plan(format!(
                "[PATH_NOT_FOUND] Path does not exist: {url}. The scheduler lists table \
                 files itself, so the location must be reachable from the scheduler, \
                 not only from the client (set ballista.planner.require_table_paths=false \
                 to read a missing location as an empty table)."
            )));
        }
    }
    Ok(())
}

async fn path_exists(ctx: &SessionContext, url: &ListingTableUrl) -> Result<bool> {
    // A local directory exists even when empty; object stores have no directories,
    // so there the location exists when it is an object or a prefix of one.
    if url.scheme() == "file" {
        let path = url.get_url().to_file_path().map_err(|_| {
            DataFusionError::Plan(format!("Invalid local table path: {url}"))
        })?;
        // With a glob the location is the directory before the first wildcard.
        let path = if url.get_glob().is_some() {
            std::path::Path::new("/").join(url.prefix().as_ref())
        } else {
            path
        };
        return Ok(path.exists());
    }
    let store = ctx.runtime_env().object_store(url)?;
    if store.head(url.prefix()).await.is_ok() {
        return Ok(true);
    }
    Ok(store.list(Some(url.prefix())).next().await.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::ParquetReadOptions;
    use std::sync::Arc;

    async fn write_table(dir: &std::path::Path) -> Result<()> {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])?;
        ctx.read_batch(batch)?
            .write_parquet(dir.to_str().unwrap(), Default::default(), None)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn existing_table_path_passes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        write_table(dir.path()).await?;
        let ctx = SessionContext::new();
        ctx.register_parquet("t", dir.path().to_str().unwrap(), ParquetReadOptions::default())
            .await?;
        let plan = ctx
            .sql("select count(*) from t where a in (select a from t)")
            .await?
            .into_unoptimized_plan();
        check_table_paths_exist(&ctx, &plan).await
    }

    /// The client registered the table while its files existed; on the scheduler the
    /// location is gone. Planning must fail instead of reading an empty table.
    #[tokio::test]
    async fn missing_table_path_fails() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let table = dir.path().join("t");
        write_table(&table).await?;
        let ctx = SessionContext::new();
        ctx.register_parquet("t", table.to_str().unwrap(), ParquetReadOptions::default())
            .await?;
        // Also inside a subquery.
        let plan = ctx
            .sql("select 1 where exists (select a from t)")
            .await?
            .into_unoptimized_plan();
        std::fs::remove_dir_all(&table)?;

        let err = check_table_paths_exist(&ctx, &plan).await.unwrap_err();
        assert!(err.to_string().contains("PATH_NOT_FOUND"), "{err}");
        Ok(())
    }

    /// An existing but empty local directory is an empty table, as in Spark.
    #[tokio::test]
    async fn empty_directory_passes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = SessionContext::new();
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        ctx.register_parquet(
            "t",
            &format!("{}/", dir.path().to_str().unwrap()),
            ParquetReadOptions::default().schema(&schema),
        )
        .await?;
        let plan = ctx.sql("select * from t").await?.into_unoptimized_plan();
        check_table_paths_exist(&ctx, &plan).await
    }
}
