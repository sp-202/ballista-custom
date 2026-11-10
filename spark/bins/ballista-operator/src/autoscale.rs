//! Executor autoscaling, modelled on Spark's `ExecutorAllocationManager`.
//!
//! - **Up:** when tasks have waited for a slot longer than
//!   `schedulerBacklogTimeout`, ask for enough executors to run every pending and
//!   running task at once (in vcores), capped at `maxExecutors`.
//! - **Down:** an executor with no running task for `executorIdleTimeout` is
//!   removed, never below `minExecutors` or below what the running tasks need. An
//!   executor holding output of a still-running job is kept (shuffle tracking)
//!   unless `shuffleTrackingTimeout` has passed.
//! - Executors still starting count as capacity, so a slow node pool does not
//!   cause a pile-up of pod requests.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use crate::crd::DynamicAllocation;

/// Mirror of the scheduler's `GET /api/scaling` response.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Scaling {
    pub pending_jobs: usize,
    pub running_jobs: usize,
    pub pending_vcores: usize,
    pub busy_vcores: u64,
    pub total_vcores: u64,
    pub executors: Vec<ScalingExecutor>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ScalingExecutor {
    pub id: String,
    pub running_tasks: usize,
    pub terminating: bool,
    pub holds_shuffle_data: bool,
}

/// An executor pod that is not being deleted.
#[derive(Clone, Debug)]
pub struct ExecutorPod {
    pub name: String,
    /// When the operator first saw it idle (pod annotation).
    pub idle_since: Option<DateTime<Utc>>,
}

#[derive(Debug, PartialEq)]
pub struct Decision {
    /// Executor pods wanted.
    pub desired: u32,
    /// Pods to create now.
    pub create: u32,
    /// Idle pods to remove now, oldest idle first.
    pub remove: Vec<String>,
    /// New `idle_since` per pod (None = busy or unknown, clear the mark).
    pub idle_marks: HashMap<String, Option<DateTime<Utc>>>,
    pub backlog_since: Option<DateTime<Utc>>,
}

pub fn decide(
    now: DateTime<Utc>,
    policy: &DynamicAllocation,
    vcores_per_executor: u32,
    pods: &[ExecutorPod],
    scaling: Option<&Scaling>,
    backlog_since: Option<DateTime<Utc>>,
) -> Decision {
    let min = policy.min_executors;
    let max = policy.max_executors.max(min);
    let current = pods.len() as u32;
    let initial = policy.initial_executors.unwrap_or(min).clamp(min, max);

    let Some(scaling) = scaling else {
        // Scheduler not reachable yet: start the initial executors, change nothing else.
        let desired = current.max(initial).min(max.max(current));
        return Decision {
            desired,
            create: desired.saturating_sub(current),
            remove: vec![],
            idle_marks: HashMap::new(),
            backlog_since,
        };
    };

    let vcores = u64::from(vcores_per_executor.max(1));
    let demand = scaling.pending_vcores as u64 + scaling.busy_vcores;
    let mut needed = demand.div_ceil(vcores) as u32;
    if needed == 0 && (scaling.pending_jobs > 0 || scaling.running_jobs > 0) {
        // A queued job has no tasks yet but will need somewhere to run.
        needed = 1;
    }
    let needed = needed.min(max);

    // Scale up after the backlog has lasted schedulerBacklogTimeout.
    let backlogged = scaling.pending_vcores > 0 || (scaling.pending_jobs > 0 && current == 0);
    let backlog_since = if backlogged {
        Some(backlog_since.unwrap_or(now))
    } else {
        None
    };
    let backlog_due = backlog_since.is_some_and(|since| {
        now - since >= Duration::seconds(policy.scheduler_backlog_timeout_seconds as i64)
    });

    let mut desired = current.clamp(min, max.max(current));
    if backlog_due {
        desired = desired.max(needed);
    }

    // Idle tracking and scale down.
    let by_id: HashMap<&str, &ScalingExecutor> = scaling
        .executors
        .iter()
        .map(|e| (e.id.as_str(), e))
        .collect();
    let mut idle_marks = HashMap::new();
    let mut removable: Vec<(DateTime<Utc>, &str)> = vec![];
    for pod in pods {
        let Some(exec) = by_id.get(pod.name.as_str()) else {
            // Not registered yet (starting) or already gone: leave it alone.
            idle_marks.insert(pod.name.clone(), None);
            continue;
        };
        if exec.running_tasks > 0 || exec.terminating {
            idle_marks.insert(pod.name.clone(), None);
            continue;
        }
        let since = pod.idle_since.unwrap_or(now);
        idle_marks.insert(pod.name.clone(), Some(since));
        let idle_for = now - since;
        if idle_for < Duration::seconds(policy.executor_idle_timeout_seconds as i64) {
            continue;
        }
        if exec.holds_shuffle_data {
            match policy.shuffle_tracking_timeout_seconds {
                Some(t) if idle_for >= Duration::seconds(t as i64) => {}
                _ => continue,
            }
        }
        removable.push((since, pod.name.as_str()));
    }

    let floor = min.max(needed);
    let mut remove = vec![];
    if !backlog_due && desired > floor {
        removable.sort();
        let excess = (desired - floor) as usize;
        remove = removable
            .into_iter()
            .take(excess)
            .map(|(_, n)| n.to_string())
            .collect();
        desired -= remove.len() as u32;
    }
    for name in &remove {
        idle_marks.remove(name);
    }

    Decision {
        desired,
        create: desired.saturating_sub(current - remove.len() as u32),
        remove,
        idle_marks,
        backlog_since,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> DynamicAllocation {
        DynamicAllocation {
            enabled: true,
            min_executors: 1,
            max_executors: 5,
            initial_executors: None,
            scheduler_backlog_timeout_seconds: 2,
            executor_idle_timeout_seconds: 60,
            shuffle_tracking_timeout_seconds: None,
        }
    }

    fn pod(name: &str, idle_since: Option<DateTime<Utc>>) -> ExecutorPod {
        ExecutorPod {
            name: name.to_string(),
            idle_since,
        }
    }

    fn exec(id: &str, running: usize, holds: bool) -> ScalingExecutor {
        ScalingExecutor {
            id: id.to_string(),
            running_tasks: running,
            terminating: false,
            holds_shuffle_data: holds,
        }
    }

    fn t0() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap_or_default()
    }

    #[test]
    fn starts_initial_executors_before_scheduler_answers() {
        let mut p = policy();
        p.initial_executors = Some(3);
        let d = decide(t0(), &p, 4, &[], None, None);
        assert_eq!((d.desired, d.create), (3, 3));
    }

    #[test]
    fn backlog_waits_for_timeout_then_scales_to_demand() {
        let p = policy();
        let pods = [pod("e1", None)];
        let s = Scaling {
            running_jobs: 1,
            pending_vcores: 10,
            busy_vcores: 4,
            executors: vec![exec("e1", 4, false)],
            ..Default::default()
        };
        // First sight of the backlog: remember it, no scale yet.
        let d = decide(t0(), &p, 4, &pods, Some(&s), None);
        assert_eq!((d.desired, d.create), (1, 0));
        assert_eq!(d.backlog_since, Some(t0()));
        // After the timeout: (10 + 4) vcores / 4 per executor = 4 executors.
        let later = t0() + Duration::seconds(3);
        let d = decide(later, &p, 4, &pods, Some(&s), d.backlog_since);
        assert_eq!((d.desired, d.create), (4, 3));
    }

    #[test]
    fn scale_up_is_capped_at_max() {
        let p = policy();
        let s = Scaling {
            pending_vcores: 1000,
            ..Default::default()
        };
        let d = decide(
            t0(),
            &p,
            4,
            &[pod("e1", None)],
            Some(&s),
            Some(t0() - Duration::seconds(10)),
        );
        assert_eq!((d.desired, d.create), (5, 4));
    }

    #[test]
    fn queued_job_wakes_up_an_empty_cluster() {
        let mut p = policy();
        p.min_executors = 0;
        let s = Scaling {
            pending_jobs: 1,
            ..Default::default()
        };
        let d = decide(
            t0(),
            &p,
            4,
            &[],
            Some(&s),
            Some(t0() - Duration::seconds(5)),
        );
        assert_eq!((d.desired, d.create), (1, 1));
    }

    #[test]
    fn idle_executors_removed_after_timeout_down_to_min() {
        let p = policy();
        let old = t0() - Duration::seconds(120);
        let pods = [pod("e1", Some(old)), pod("e2", Some(old)), pod("e3", None)];
        let s = Scaling {
            executors: vec![
                exec("e1", 0, false),
                exec("e2", 0, false),
                exec("e3", 0, false),
            ],
            ..Default::default()
        };
        let d = decide(t0(), &p, 4, &pods, Some(&s), None);
        // e3 just became idle; e1 and e2 are over the timeout; min is 1, so both go.
        assert_eq!(d.remove, vec!["e1".to_string(), "e2".to_string()]);
        assert_eq!((d.desired, d.create), (1, 0));
        assert_eq!(d.idle_marks.get("e3"), Some(&Some(t0())));
    }

    #[test]
    fn never_below_min() {
        let mut p = policy();
        p.min_executors = 2;
        let old = t0() - Duration::seconds(120);
        let pods = [pod("e1", Some(old)), pod("e2", Some(old))];
        let s = Scaling {
            executors: vec![exec("e1", 0, false), exec("e2", 0, false)],
            ..Default::default()
        };
        let d = decide(t0(), &p, 4, &pods, Some(&s), None);
        assert!(d.remove.is_empty());
        assert_eq!(d.desired, 2);
    }

    #[test]
    fn busy_executor_is_not_removed_and_mark_cleared() {
        let p = policy();
        let old = t0() - Duration::seconds(120);
        let pods = [pod("e1", Some(old)), pod("e2", Some(old))];
        let s = Scaling {
            running_jobs: 1,
            busy_vcores: 1,
            executors: vec![exec("e1", 1, false), exec("e2", 0, false)],
            ..Default::default()
        };
        let d = decide(t0(), &p, 4, &pods, Some(&s), None);
        assert_eq!(d.remove, vec!["e2".to_string()]);
        assert_eq!(d.idle_marks.get("e1"), Some(&None));
    }

    #[test]
    fn shuffle_holder_kept_unless_tracking_timeout_passed() {
        let mut p = policy();
        let old = t0() - Duration::seconds(120);
        let pods = [pod("e1", Some(old)), pod("e2", Some(old))];
        let s = Scaling {
            running_jobs: 1,
            busy_vcores: 1,
            executors: vec![exec("e1", 1, false), exec("e2", 0, true)],
            ..Default::default()
        };
        let d = decide(t0(), &p, 4, &pods, Some(&s), None);
        assert!(
            d.remove.is_empty(),
            "holder of a running job's output is kept"
        );
        p.shuffle_tracking_timeout_seconds = Some(90);
        let d = decide(t0(), &p, 4, &pods, Some(&s), None);
        assert_eq!(d.remove, vec!["e2".to_string()]);
    }

    #[test]
    fn starting_pods_count_as_capacity_and_are_not_removed() {
        let p = policy();
        // e2 is not registered yet; a backlog that 2 executors cover adds none.
        let pods = [pod("e1", None), pod("e2", None)];
        let s = Scaling {
            running_jobs: 1,
            pending_vcores: 4,
            busy_vcores: 4,
            executors: vec![exec("e1", 4, false)],
            ..Default::default()
        };
        let d = decide(
            t0(),
            &p,
            4,
            &pods,
            Some(&s),
            Some(t0() - Duration::seconds(5)),
        );
        assert_eq!((d.desired, d.create), (2, 0));
        assert!(d.remove.is_empty());
    }

    #[test]
    fn no_removal_while_backlogged() {
        let p = policy();
        let old = t0() - Duration::seconds(120);
        let pods = [pod("e1", Some(old)), pod("e2", None)];
        let s = Scaling {
            running_jobs: 1,
            pending_vcores: 2,
            executors: vec![exec("e1", 0, false), exec("e2", 0, false)],
            ..Default::default()
        };
        let d = decide(
            t0(),
            &p,
            4,
            &pods,
            Some(&s),
            Some(t0() - Duration::seconds(5)),
        );
        assert!(d.remove.is_empty());
    }
}
