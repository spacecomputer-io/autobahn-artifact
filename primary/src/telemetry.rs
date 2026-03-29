//! Lightweight experiment telemetry: event CSV + fast state CSV.
//!
//! These outputs supplement the existing Prometheus metrics with:
//! - sub-second state snapshots for recovery-critical gauges
//! - transition-driven events for partition/recovery analysis

use lazy_static::lazy_static;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// ── Event logger ────────────────────────────────────────────────

lazy_static! {
    static ref EVENT_WRITER: Mutex<Option<EventState>> = Mutex::new(None);
}

struct EventState {
    writer: BufWriter<File>,
    role: String,
    node: String,
}

/// Initialize the global event logger. Call once at process startup.
pub fn init_event_logger(path: &str, role: &str, node: &str) {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap_or_else(|e| panic!("Failed to open events CSV at {}: {}", path, e));
    let mut writer = BufWriter::new(file);
    writeln!(writer, "ts_ms,role,node,event,value").expect("Failed to write events CSV header");
    writer.flush().expect("Failed to flush events CSV header");
    *EVENT_WRITER.lock().unwrap() = Some(EventState {
        writer,
        role: role.to_string(),
        node: node.to_string(),
    });
}

/// Emit a single event row. Safe to call from any thread/task.
/// No-op if the event logger has not been initialized.
pub fn emit_event(event: &str, value: &str) {
    if let Ok(mut guard) = EVENT_WRITER.lock() {
        if let Some(state) = guard.as_mut() {
            let ts = now_ms();
            let _ = writeln!(
                state.writer,
                "{},{},{},{},{}",
                ts, state.role, state.node, event, value
            );
            let _ = state.writer.flush();
        }
    }
}

// ── Fast state writer ───────────────────────────────────────────

/// Spawn a tokio task that samples critical primary gauges at `interval_ms`
/// and writes them to a local CSV file.
pub fn spawn_primary_fast_state(path: &str, node: &str, interval_ms: u64) {
    use crate::metrics::{
        CONSENSUS_CURRENT_SLOT, CONSENSUS_CURRENT_VIEW, CONSENSUS_OLDEST_BLOCKED_SLOT,
        DISSEMINATION_HOLE_DEPENDENTS, DISSEMINATION_INFLIGHT_HOLES,
    };
    use tokio::time::{sleep, Duration, Instant};

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap_or_else(|e| panic!("Failed to open fast_state CSV at {}: {}", path, e));
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "ts_ms,role,node,consensus_current_slot,consensus_oldest_blocked_slot,consensus_current_view,dissemination_inflight_holes,dissemination_hole_dependents"
    )
    .expect("Failed to write fast_state CSV header");
    writer.flush().expect("Failed to flush fast_state CSV header");

    let node = node.to_string();
    let interval = Duration::from_millis(interval_ms);

    tokio::spawn(async move {
        let mut next = Instant::now() + interval;
        loop {
            sleep(next.saturating_duration_since(Instant::now())).await;
            next += interval;

            let ts = now_ms();
            let slot = CONSENSUS_CURRENT_SLOT.get();
            let blocked = CONSENSUS_OLDEST_BLOCKED_SLOT.get();
            let view = CONSENSUS_CURRENT_VIEW.get();
            let holes = DISSEMINATION_INFLIGHT_HOLES.get();
            let deps = DISSEMINATION_HOLE_DEPENDENTS.get();

            let _ = writeln!(
                writer,
                "{},primary,{},{},{},{},{},{}",
                ts, node, slot, blocked, view, holes, deps
            );
            // Flush every sample to ensure data is available if the process crashes.
            let _ = writer.flush();
        }
    });
}

/// Spawn a tokio task that samples primary counters/gauges and emits
/// transition events without modifying protocol logic.
pub fn spawn_primary_event_sampler(interval_ms: u64) {
    use crate::metrics::{
        CONSENSUS_CURRENT_VIEW, CONSENSUS_FAST_PATH_COMMITS_TOTAL, CONSENSUS_LEADER_CHANGES_TOTAL,
        CONSENSUS_OLDEST_BLOCKED_SLOT, CONSENSUS_SLOW_PATH_COMMITS_TOTAL,
        CONSENSUS_TIMEOUTS_AS_LEADER_TOTAL, CONSENSUS_VIEW_CHANGES_TOTAL,
    };
    use tokio::time::{sleep, Duration, Instant};

    let interval = Duration::from_millis(interval_ms);

    tokio::spawn(async move {
        let mut prev_blocked = CONSENSUS_OLDEST_BLOCKED_SLOT.get();
        let mut prev_view = CONSENSUS_CURRENT_VIEW.get();
        let mut prev_fast = CONSENSUS_FAST_PATH_COMMITS_TOTAL.get();
        let mut prev_slow = CONSENSUS_SLOW_PATH_COMMITS_TOTAL.get();
        let mut prev_view_changes = CONSENSUS_VIEW_CHANGES_TOTAL.get();
        let mut prev_leader_changes = CONSENSUS_LEADER_CHANGES_TOTAL.get();
        let mut prev_leader_timeouts = CONSENSUS_TIMEOUTS_AS_LEADER_TOTAL.get();

        let mut next = Instant::now() + interval;
        loop {
            sleep(next.saturating_duration_since(Instant::now())).await;
            next += interval;

            let blocked = CONSENSUS_OLDEST_BLOCKED_SLOT.get();
            if prev_blocked == 0 && blocked > 0 {
                emit_event("blocked_slot_enter", &blocked.to_string());
                emit_event("recovery_start", &blocked.to_string());
            } else if prev_blocked > 0 && blocked == 0 {
                emit_event("blocked_slot_exit", &prev_blocked.to_string());
                emit_event("recovery_end", "0");
            }
            prev_blocked = blocked;

            let view = CONSENSUS_CURRENT_VIEW.get();
            if view != prev_view {
                emit_event("view_change", &view.to_string());
            }
            prev_view = view;

            let fast = CONSENSUS_FAST_PATH_COMMITS_TOTAL.get();
            if fast > prev_fast {
                emit_event("fast_path_commit", &(fast - prev_fast).to_string());
            }
            prev_fast = fast;

            let slow = CONSENSUS_SLOW_PATH_COMMITS_TOTAL.get();
            if slow > prev_slow {
                emit_event("slow_path_commit", &(slow - prev_slow).to_string());
            }
            prev_slow = slow;

            let view_changes = CONSENSUS_VIEW_CHANGES_TOTAL.get();
            if view_changes > prev_view_changes {
                emit_event("view_change_total", &(view_changes - prev_view_changes).to_string());
            }
            prev_view_changes = view_changes;

            let leader_changes = CONSENSUS_LEADER_CHANGES_TOTAL.get();
            if leader_changes > prev_leader_changes {
                emit_event("leader_change", &(leader_changes - prev_leader_changes).to_string());
            }
            prev_leader_changes = leader_changes;

            let leader_timeouts = CONSENSUS_TIMEOUTS_AS_LEADER_TOTAL.get();
            if leader_timeouts > prev_leader_timeouts {
                emit_event("leader_timeout", &(leader_timeouts - prev_leader_timeouts).to_string());
            }
            prev_leader_timeouts = leader_timeouts;
        }
    });
}

// Worker fast state writer lives in node/src/main.rs because the primary
// crate cannot depend on the worker crate.

// ── Helpers ─────────────────────────────────────────────────────

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to measure time")
        .as_millis()
}
