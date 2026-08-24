//! Promoter metrics: atomic counters + gauges for observability.
//!
//! Kept intentionally simple (no external metrics crate dependency): all
//! state lives behind `AtomicU64` so any thread can update without a lock,
//! and callers `snapshot()` for logging or export.

use crate::promoter::lifecycle::FailureKind;
use std::sync::atomic::{AtomicU64, Ordering};

/// Aggregate counters/gauges maintained by the promoter orchestrator. All
/// fields are monotonic counters except those explicitly named `*_gauge`.
#[derive(Debug, Default)]
pub struct PromoterMetrics {
    ticks_total: AtomicU64,
    mints_promoted_total: AtomicU64,
    mints_demoted_total: AtomicU64,
    grpc_resubscribes_total: AtomicU64,
    grpc_resubscribe_errors_total: AtomicU64,

    // Lifecycle failure counters, one per `FailureKind`. Ordered
    // Discovery, AtaCreation, AltExtension, RegistryAdmit, GrpcSubscribe.
    lifecycle_failures_total: [AtomicU64; 5],

    // Gauges: latest observed value, not monotonic.
    current_active_count_gauge: AtomicU64,
    current_cooling_count_gauge: AtomicU64,
    last_tick_duration_ms_gauge: AtomicU64,
}

impl PromoterMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_tick(&self, duration_ms: u64) {
        self.ticks_total.fetch_add(1, Ordering::Relaxed);
        self.last_tick_duration_ms_gauge
            .store(duration_ms, Ordering::Relaxed);
    }

    pub fn record_promoted(&self) {
        self.mints_promoted_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_demoted(&self) {
        self.mints_demoted_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_resubscribe_ok(&self) {
        self.grpc_resubscribes_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_resubscribe_err(&self) {
        self.grpc_resubscribe_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self, kind: FailureKind) {
        let idx = failure_index(kind);
        self.lifecycle_failures_total[idx].fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_active_count(&self, count: u64) {
        self.current_active_count_gauge
            .store(count, Ordering::Relaxed);
    }

    pub fn set_cooling_count(&self, count: u64) {
        self.current_cooling_count_gauge
            .store(count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PromoterMetricsSnapshot {
        PromoterMetricsSnapshot {
            ticks_total: self.ticks_total.load(Ordering::Relaxed),
            mints_promoted_total: self.mints_promoted_total.load(Ordering::Relaxed),
            mints_demoted_total: self.mints_demoted_total.load(Ordering::Relaxed),
            grpc_resubscribes_total: self.grpc_resubscribes_total.load(Ordering::Relaxed),
            grpc_resubscribe_errors_total: self
                .grpc_resubscribe_errors_total
                .load(Ordering::Relaxed),
            lifecycle_failures_total: [
                self.lifecycle_failures_total[0].load(Ordering::Relaxed),
                self.lifecycle_failures_total[1].load(Ordering::Relaxed),
                self.lifecycle_failures_total[2].load(Ordering::Relaxed),
                self.lifecycle_failures_total[3].load(Ordering::Relaxed),
                self.lifecycle_failures_total[4].load(Ordering::Relaxed),
            ],
            current_active_count: self.current_active_count_gauge.load(Ordering::Relaxed),
            current_cooling_count: self.current_cooling_count_gauge.load(Ordering::Relaxed),
            last_tick_duration_ms: self.last_tick_duration_ms_gauge.load(Ordering::Relaxed),
        }
    }
}

fn failure_index(kind: FailureKind) -> usize {
    match kind {
        FailureKind::Discovery => 0,
        FailureKind::AtaCreation => 1,
        FailureKind::AltExtension => 2,
        FailureKind::RegistryAdmit => 3,
        FailureKind::GrpcSubscribe => 4,
    }
}

/// Point-in-time snapshot suitable for logging or JSON export.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PromoterMetricsSnapshot {
    pub ticks_total: u64,
    pub mints_promoted_total: u64,
    pub mints_demoted_total: u64,
    pub grpc_resubscribes_total: u64,
    pub grpc_resubscribe_errors_total: u64,
    /// Indexed by `FailureKind`: [Discovery, AtaCreation, AltExtension,
    /// RegistryAdmit, GrpcSubscribe].
    pub lifecycle_failures_total: [u64; 5],
    pub current_active_count: u64,
    pub current_cooling_count: u64,
    pub last_tick_duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        let m = PromoterMetrics::new();
        let s = m.snapshot();
        assert_eq!(s.ticks_total, 0);
        assert_eq!(s.mints_promoted_total, 0);
        assert_eq!(s.lifecycle_failures_total, [0; 5]);
    }

    #[test]
    fn counters_increment_independently() {
        let m = PromoterMetrics::new();
        m.record_promoted();
        m.record_promoted();
        m.record_demoted();
        m.record_failure(FailureKind::Discovery);
        m.record_failure(FailureKind::GrpcSubscribe);
        m.record_failure(FailureKind::GrpcSubscribe);
        m.record_resubscribe_ok();
        m.record_resubscribe_err();
        m.record_tick(42);

        let s = m.snapshot();
        assert_eq!(s.mints_promoted_total, 2);
        assert_eq!(s.mints_demoted_total, 1);
        assert_eq!(s.lifecycle_failures_total[0], 1); // Discovery
        assert_eq!(s.lifecycle_failures_total[4], 2); // GrpcSubscribe
        assert_eq!(s.grpc_resubscribes_total, 1);
        assert_eq!(s.grpc_resubscribe_errors_total, 1);
        assert_eq!(s.ticks_total, 1);
        assert_eq!(s.last_tick_duration_ms, 42);
    }

    #[test]
    fn gauges_overwrite_previous_value() {
        let m = PromoterMetrics::new();
        m.set_active_count(3);
        m.set_active_count(7);
        m.set_cooling_count(2);
        m.set_cooling_count(1);
        let s = m.snapshot();
        assert_eq!(s.current_active_count, 7);
        assert_eq!(s.current_cooling_count, 1);
    }
}
