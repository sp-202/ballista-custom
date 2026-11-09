//! Spark settings that engine options implement.
//!
//! A Spark setting such as `spark.sql.shuffle.partitions` is stored in the Spark
//! session, but only does something once the option that implements it is set on
//! the DataFusion session: its `target_partitions` decides how many partitions a
//! shuffle has, and the Ballista scheduler reads its `ballista.planner.*` keys from
//! the job settings. [`apply`] copies a Spark setting to those options.
//!
//! Keys the engine does not know are skipped. In Sail's own (local) mode the
//! `ballista.*` namespace is not registered, so setting one there does nothing.

use datafusion::config::ConfigOptions;
use log::{debug, warn};

pub const SHUFFLE_PARTITIONS: &str = "spark.sql.shuffle.partitions";
const ADAPTIVE_ENABLED: &str = "spark.sql.adaptive.enabled";
const COALESCE_ENABLED: &str = "spark.sql.adaptive.coalescePartitions.enabled";
const ADVISORY_PARTITION_SIZE: &str = "spark.sql.adaptive.advisoryPartitionSizeInBytes";
const SKEW_JOIN_ENABLED: &str = "spark.sql.adaptive.skewJoin.enabled";
const SKEWED_PARTITION_FACTOR: &str = "spark.sql.adaptive.skewJoin.skewedPartitionFactor";
const SKEWED_PARTITION_THRESHOLD: &str =
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes";
const AUTO_BROADCAST_THRESHOLD: &str = "spark.sql.autoBroadcastJoinThreshold";
const DYNAMIC_PARTITION_PRUNING: &str = "spark.sql.optimizer.dynamicPartitionPruning.enabled";

const TARGET_PARTITIONS: &str = "datafusion.execution.target_partitions";
const AQE_PHYSICAL_PLANS: &str = "ballista.planner.adaptive.physical_plans.enabled";
const BALLISTA_COALESCE_ENABLED: &str = "ballista.planner.coalesce.enabled";
const BALLISTA_COALESCE_TARGET: &str = "ballista.planner.coalesce.target_partition_bytes";
const BALLISTA_SKEW_ENABLED: &str = "ballista.planner.skew_join.enabled";
const BALLISTA_SKEW_FACTOR: &str = "ballista.planner.skew_join.skewed_partition_factor";
const BALLISTA_SKEW_THRESHOLD: &str =
    "ballista.planner.skew_join.skewed_partition_threshold_bytes";
const BALLISTA_BROADCAST_THRESHOLD: &str = "ballista.optimizer.broadcast_join_threshold_bytes";
const BALLISTA_DPP: &str = "ballista.executor.dynamic_partition_pruning.enabled";

/// Spark's default for each setting that [`apply`] handles, as Spark 3.5 documents it.
///
/// A session starts with these (see [`apply_defaults`]) and returns to them when a
/// client unsets a key.
pub fn spark_default(key: &str) -> Option<&'static str> {
    Some(match key {
        SHUFFLE_PARTITIONS => "200",
        ADAPTIVE_ENABLED => "true",
        COALESCE_ENABLED => "true",
        ADVISORY_PARTITION_SIZE => "64MB",
        SKEW_JOIN_ENABLED => "true",
        SKEWED_PARTITION_FACTOR => "5.0",
        SKEWED_PARTITION_THRESHOLD => "256MB",
        AUTO_BROADCAST_THRESHOLD => "10MB",
        DYNAMIC_PARTITION_PRUNING => "true",
        _ => return None,
    })
}

/// Shuffle partitions of a new session when the operator has not set a count.
///
/// Without it a session shuffles into one partition per CPU core of the machine that
/// plans the query (28 on the benchmark host, against 16 task slots), however large
/// the data is. On TPC-H SF100 (2 executors, 16 slots, whole 22-query suite in one
/// session) 28 partitions took 307 s, 64 took 252 s, 100 took 228 s, and 200 took 254 s
/// because queries over small tables slowed down. Spark's 200 assumes AQE coalescing
/// brings the count back down; Ballista's coalescing made no measurable difference, so
/// 100 (about 6 per task slot) is the default until the count follows the input size.
pub const DEFAULT_SHUFFLE_PARTITIONS: usize = 100;

/// Gives a new session a shuffle partition count suited to a cluster rather than to the
/// planning machine's CPU count. With `keep_parallelism` the count is left as
/// configured, for a session whose operator set `execution.default_parallelism`.
pub fn apply_defaults(options: &mut ConfigOptions, keep_parallelism: bool) {
    if !keep_parallelism {
        options.execution.target_partitions = DEFAULT_SHUFFLE_PARTITIONS;
    }
}

/// Copies a Spark setting to the engine options that implement it.
///
/// Returns whether the key is one that has engine options. An invalid value is logged
/// and ignored, so the session keeps working with the previous one.
pub fn apply(options: &mut ConfigOptions, key: &str, value: &str) -> bool {
    let Some(settings) = engine_options(key, value) else {
        return false;
    };
    for (name, value) in settings {
        match options.set(name, &value) {
            Ok(()) => debug!("Spark setting {key}={value}: set {name}"),
            // The namespace is not registered (Sail's local mode has no `ballista.*`).
            Err(e) => debug!("Spark setting {key}: could not set {name}: {e}"),
        }
    }
    true
}

/// The engine options for a Spark setting, or `None` for a key that has none.
/// An empty list means the value is valid but nothing needs to change.
fn engine_options(key: &str, value: &str) -> Option<Vec<(&'static str, String)>> {
    let value = value.trim();
    let bool_value = |v: &str| -> Option<String> {
        match v.to_ascii_lowercase().as_str() {
            "true" => Some("true".to_string()),
            "false" => Some("false".to_string()),
            _ => None,
        }
    };
    let (name, converted): (&'static str, Option<String>) = match key {
        SHUFFLE_PARTITIONS => (
            TARGET_PARTITIONS,
            value.parse::<usize>().ok().filter(|n| *n > 0).map(|n| n.to_string()),
        ),
        ADAPTIVE_ENABLED => (AQE_PHYSICAL_PLANS, bool_value(value)),
        COALESCE_ENABLED => (BALLISTA_COALESCE_ENABLED, bool_value(value)),
        ADVISORY_PARTITION_SIZE => (
            BALLISTA_COALESCE_TARGET,
            parse_bytes(value).filter(|n| *n > 0).map(|n| n.to_string()),
        ),
        SKEW_JOIN_ENABLED => (BALLISTA_SKEW_ENABLED, bool_value(value)),
        SKEWED_PARTITION_FACTOR => (
            BALLISTA_SKEW_FACTOR,
            value.parse::<f64>().ok().filter(|f| *f > 0.0).map(|f| f.to_string()),
        ),
        SKEWED_PARTITION_THRESHOLD => (
            BALLISTA_SKEW_THRESHOLD,
            parse_bytes(value).map(|n| n.to_string()),
        ),
        // Spark: -1 turns broadcast joins off. Ballista: 0 does.
        AUTO_BROADCAST_THRESHOLD => (
            BALLISTA_BROADCAST_THRESHOLD,
            if value.starts_with('-') {
                Some("0".to_string())
            } else {
                parse_bytes(value).map(|n| n.to_string())
            },
        ),
        DYNAMIC_PARTITION_PRUNING => (BALLISTA_DPP, bool_value(value)),
        _ => return None,
    };
    match converted {
        Some(v) => Some(vec![(name, v)]),
        None => {
            warn!("ignoring invalid value {value:?} for {key}");
            Some(Vec::new())
        }
    }
}

/// Parses a Spark byte size: a number with an optional unit (`b`, `k`/`kb`, `m`/`mb`,
/// `g`/`gb`, `t`/`tb`; powers of 1024, case-insensitive). No unit means bytes.
fn parse_bytes(value: &str) -> Option<u64> {
    let value = value.trim().to_ascii_lowercase();
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let number = number.parse::<u64>().ok()?;
    let factor: u64 = match unit.trim() {
        "" | "b" => 1,
        "k" | "kb" => 1 << 10,
        "m" | "mb" => 1 << 20,
        "g" | "gb" => 1 << 30,
        "t" | "tb" => 1 << 40,
        _ => return None,
    };
    number.checked_mul(factor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_byte_sizes() {
        assert_eq!(parse_bytes("64MB"), Some(64 << 20));
        assert_eq!(parse_bytes("64m"), Some(64 << 20));
        assert_eq!(parse_bytes("1g"), Some(1 << 30));
        assert_eq!(parse_bytes("1024"), Some(1024));
        assert_eq!(parse_bytes("10 KB"), Some(10 << 10));
        assert_eq!(parse_bytes("ten"), None);
        assert_eq!(parse_bytes("5xb"), None);
    }

    #[test]
    fn shuffle_partitions_sets_target_partitions() {
        let mut options = ConfigOptions::default();
        assert!(apply(&mut options, SHUFFLE_PARTITIONS, "64"));
        assert_eq!(options.execution.target_partitions, 64);
        // Invalid values keep the previous one.
        assert!(apply(&mut options, SHUFFLE_PARTITIONS, "0"));
        assert!(apply(&mut options, SHUFFLE_PARTITIONS, "many"));
        assert_eq!(options.execution.target_partitions, 64);
    }

    #[test]
    fn unrelated_keys_are_not_handled() {
        let mut options = ConfigOptions::default();
        assert!(!apply(&mut options, "spark.sql.session.timeZone", "UTC"));
    }

    #[test]
    fn broadcast_threshold_minus_one_disables() {
        let settings = engine_options(AUTO_BROADCAST_THRESHOLD, "-1").unwrap();
        assert_eq!(settings, vec![(BALLISTA_BROADCAST_THRESHOLD, "0".to_string())]);
        let settings = engine_options(AUTO_BROADCAST_THRESHOLD, "10MB").unwrap();
        assert_eq!(
            settings,
            vec![(BALLISTA_BROADCAST_THRESHOLD, (10u64 << 20).to_string())]
        );
    }

    #[test]
    fn defaults_cover_every_handled_key() {
        for key in [
            SHUFFLE_PARTITIONS,
            ADAPTIVE_ENABLED,
            COALESCE_ENABLED,
            ADVISORY_PARTITION_SIZE,
            SKEW_JOIN_ENABLED,
            SKEWED_PARTITION_FACTOR,
            SKEWED_PARTITION_THRESHOLD,
            AUTO_BROADCAST_THRESHOLD,
            DYNAMIC_PARTITION_PRUNING,
        ] {
            let default = spark_default(key).unwrap();
            let settings = engine_options(key, default).unwrap();
            assert_eq!(settings.len(), 1, "{key}: default {default:?} must convert");
        }
    }

    #[test]
    fn session_defaults_respect_explicit_parallelism() {
        let mut options = ConfigOptions::default();
        options.execution.target_partitions = 12;
        apply_defaults(&mut options, true);
        assert_eq!(options.execution.target_partitions, 12);
        apply_defaults(&mut options, false);
        assert_eq!(options.execution.target_partitions, DEFAULT_SHUFFLE_PARTITIONS);
    }
}
