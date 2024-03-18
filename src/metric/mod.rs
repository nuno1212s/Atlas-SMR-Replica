use atlas_metrics::metrics::MetricKind;
use atlas_metrics::{MetricLevel, MetricRegistry};

/// Replica will get the 5XX metrics codes

pub const ORDERING_PROTOCOL_POLL_TIME: &str = "ORDERING_PROTOCOL_POLL_TIME";
pub const ORDERING_PROTOCOL_POLL_TIME_ID: usize = 500;

pub const ORDERING_PROTOCOL_PROCESS_TIME: &str = "ORDERING_PROTOCOL_PROCESS_TIME";
pub const ORDERING_PROTOCOL_PROCESS_TIME_ID: usize = 501;

pub const STATE_TRANSFER_PROCESS_TIME: &str = "STATE_TRANSFER_PROCESS_TIME";
pub const STATE_TRANSFER_PROCESS_TIME_ID: usize = 502;

pub const TIMEOUT_PROCESS_TIME: &str = "TIMEOUT_PROCESS_TIME";
pub const TIMEOUT_PROCESS_TIME_ID: usize = 503;

pub const APP_STATE_DIGEST_TIME: &str = "APP_STATE_DIGEST_TIME";
pub const APP_STATE_DIGEST_TIME_ID: usize = 504;

pub const EXECUTION_LATENCY_TIME: &str = "EXECUTION_LATENCY";
pub const EXECUTION_LATENCY_TIME_ID: usize = 505;

pub const EXECUTION_TIME_TAKEN: &str = "EXECUTION_TIME_TAKEN";
pub const EXECUTION_TIME_TAKEN_ID: usize = 506;

pub const RUN_LATENCY_TIME: &str = "RUN_LATENCY_TIME";
pub const RUN_LATENCY_TIME_ID: usize = 509;

pub const REPLICA_RQ_QUEUE_SIZE: &str = "REPLICA_RQ_QUEUE_SIZE";
pub const REPLICA_RQ_QUEUE_SIZE_ID: usize = 510;

pub const REPLICA_INTERNAL_PROCESS_TIME: &str = "REPLICA_INTERNAL_PROCESS_TIME";
pub const REPLICA_INTERNAL_PROCESS_TIME_ID: usize = 511;

pub const REPLICA_TAKE_FROM_NETWORK: &str = "REPLICA_TAKE_FROM_NETWORK";
pub const REPLICA_TAKE_FROM_NETWORK_ID: usize = 512;

pub const REPLICA_ORDERED_RQS_PROCESSED: &str = "REPLICA_ORDERED_RQS_PROCESSED";
pub const REPLICA_ORDERED_RQS_PROCESSED_ID: usize = 513;

pub const LOG_TRANSFER_PROCESS_TIME: &str = "LOG_TRANSFER_PROCESS_TIME";
pub const LOG_TRANSFER_PROCESS_TIME_ID: usize = 514;

pub const REPLICA_PROTOCOL_RESP_PROCESS_TIME: &str = "REPLICA_PROTOCOL_RESP_PROCESS_TIME";
pub const REPLICA_PROTOCOL_RESP_PROCESS_TIME_ID: usize = 515;

pub const STATE_TRANSFER_RUNTIME: &str = "STATE_TRANSFER_RUN_TIME";
pub const STATE_TRANSFER_RUNTIME_ID: usize = 516;

/// How many unordered operations are we pushing to the executor
pub const UNORDERED_OPS_PER_SECOND: &str = "UNORDERED_OPS_PUSHED";
pub const UNORDERED_OPS_PER_SECOND_ID: usize = 517;

pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (
            ORDERING_PROTOCOL_POLL_TIME_ID,
            ORDERING_PROTOCOL_POLL_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Trace,
        )
            .into(),
        (
            ORDERING_PROTOCOL_PROCESS_TIME_ID,
            ORDERING_PROTOCOL_PROCESS_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Debug,
        )
            .into(),
        (
            STATE_TRANSFER_PROCESS_TIME_ID,
            STATE_TRANSFER_PROCESS_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Debug,
        )
            .into(),
        (
            TIMEOUT_PROCESS_TIME_ID,
            TIMEOUT_PROCESS_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Debug,
        )
            .into(),
        (
            APP_STATE_DIGEST_TIME_ID,
            APP_STATE_DIGEST_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Info,
        )
            .into(),
        (
            EXECUTION_LATENCY_TIME_ID,
            EXECUTION_LATENCY_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Debug,
        )
            .into(),
        (
            EXECUTION_TIME_TAKEN_ID,
            EXECUTION_TIME_TAKEN.to_string(),
            MetricKind::Duration,
            MetricLevel::Debug,
        )
            .into(),
        (
            RUN_LATENCY_TIME_ID,
            RUN_LATENCY_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Debug,
        )
            .into(),
        (
            REPLICA_RQ_QUEUE_SIZE_ID,
            REPLICA_RQ_QUEUE_SIZE.to_string(),
            MetricKind::Count,
            MetricLevel::Trace,
        )
            .into(),
        (
            REPLICA_INTERNAL_PROCESS_TIME_ID,
            REPLICA_INTERNAL_PROCESS_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Trace,
        )
            .into(),
        (
            REPLICA_TAKE_FROM_NETWORK_ID,
            REPLICA_TAKE_FROM_NETWORK.to_string(),
            MetricKind::Duration,
            MetricLevel::Trace,
        )
            .into(),
        (
            REPLICA_ORDERED_RQS_PROCESSED_ID,
            REPLICA_ORDERED_RQS_PROCESSED.to_string(),
            MetricKind::Counter,
            MetricLevel::Trace,
        )
            .into(),
        (
            LOG_TRANSFER_PROCESS_TIME_ID,
            LOG_TRANSFER_PROCESS_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Debug,
        )
            .into(),
        (
            REPLICA_PROTOCOL_RESP_PROCESS_TIME_ID,
            REPLICA_PROTOCOL_RESP_PROCESS_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Debug,
        )
            .into(),
        (
            STATE_TRANSFER_PROCESS_TIME_ID,
            STATE_TRANSFER_PROCESS_TIME.to_string(),
            MetricKind::Duration,
            MetricLevel::Info,
        )
            .into(),
        (
            UNORDERED_OPS_PER_SECOND_ID,
            UNORDERED_OPS_PER_SECOND.to_string(),
            MetricKind::Counter
        )
            .into(),
    ]
}
