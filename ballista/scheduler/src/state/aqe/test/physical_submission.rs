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

//! AQE for jobs submitted as already-built physical plans
//! ([`AdaptivePlanner::try_new_from_physical`]).

use crate::state::aqe::planner::AdaptivePlanner;
use crate::state::aqe::test::{ballista_ctx, mock_batch};
use ballista_core::config::BallistaConfig;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;

const JOIN_QUERY: &str = r#"
    select t1.c, sum(t2.b) as s
    from t1 join t2 on t1.a = t2.a
    group by t1.c
    order by s desc, t1.c
"#;

fn context() -> datafusion::error::Result<SessionContext> {
    let ctx = ballista_ctx();
    ctx.register_batch("t1", mock_batch()?)?;
    ctx.register_batch("t2", mock_batch()?)?;
    Ok(ctx)
}

#[tokio::test]
async fn should_plan_optimized_physical_plan_adaptively() -> datafusion::error::Result<()>
{
    let ctx = context()?;
    let physical = ctx.sql(JOIN_QUERY).await?.create_physical_plan().await?;

    let mut planner =
        AdaptivePlanner::try_new_from_physical(&ctx, physical, "physical-job".into())?;
    let plan = displayable(planner.current_plan()).indent(true).to_string();

    assert!(plan.starts_with("AdaptiveDatafusionExec"), "{plan}");
    assert!(plan.contains("ExchangeExec"), "{plan}");
    assert!(
        planner.runnable_stages()?.is_some(),
        "a new job must have runnable stages:\n{plan}"
    );
    Ok(())
}

/// A physical plan arrives with its global ordering already established (a
/// final merge of sorted partitions). The adaptive planner must keep it, or
/// `ORDER BY` results would come back unordered.
#[tokio::test]
async fn should_keep_global_ordering_of_physical_plan() -> datafusion::error::Result<()> {
    let ctx = context()?;
    let physical = ctx.sql(JOIN_QUERY).await?.create_physical_plan().await?;

    let planner =
        AdaptivePlanner::try_new_from_physical(&ctx, physical, "physical-job".into())?;
    let plan = displayable(planner.current_plan()).indent(true).to_string();

    let globally_sorted = plan.contains("SortPreservingMergeExec")
        || plan.contains("preserve_partitioning=[false]");
    assert!(globally_sorted, "global ordering lost:\n{plan}");
    Ok(())
}

/// The same query planned from its logical plan and from its optimized
/// physical plan must both produce an adaptive plan with exchanges.
#[tokio::test]
async fn should_plan_physical_like_logical() -> datafusion::error::Result<()> {
    let ctx = context()?;
    let logical = ctx.sql(JOIN_QUERY).await?.into_optimized_plan()?;
    let physical = ctx.sql(JOIN_QUERY).await?.create_physical_plan().await?;

    let from_logical = AdaptivePlanner::try_new(&ctx, &logical, "logical-job".into()).await?;
    let from_physical =
        AdaptivePlanner::try_new_from_physical(&ctx, physical, "physical-job".into())?;

    for planner in [&from_logical, &from_physical] {
        let plan = displayable(planner.current_plan()).indent(true).to_string();
        assert!(plan.starts_with("AdaptiveDatafusionExec"), "{plan}");
        assert!(plan.contains("ExchangeExec"), "{plan}");
    }
    Ok(())
}

#[test]
fn physical_plan_aqe_is_disabled_by_default() {
    assert!(!BallistaConfig::default().adaptive_physical_plans_enabled());
}
