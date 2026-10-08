//! Prometheus text-format metrics (contract C51): `gateway_sessions_open`,
//! `gateway_messages_total`, `gateway_queue_depth{skill}`, `gateway_delivery_seconds`.
//! Counters are per node; queue depth is cluster-wide (read from the store at scrape time).

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use super::super::domain::{Channel, Direction};

/// Histogram buckets in seconds for inbound-to-session delivery.
pub const BUCKETS: [f64; 12] = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];

#[derive(Default)]
pub struct Metrics {
    /// whatsapp/inbound, whatsapp/outbound, sip/inbound, sip/outbound
    messages: [AtomicU64; 4],
    duplicates: AtomicU64,
    buckets: [AtomicU64; 12],
    delivery_count: AtomicU64,
    delivery_sum_micros: AtomicU64,
}

fn idx(c: Channel, d: Direction) -> usize {
    (match c {
        Channel::Whatsapp => 0,
        Channel::Sip => 2,
    }) + match d {
        Direction::Inbound => 0,
        Direction::Outbound => 1,
    }
}

impl Metrics {
    pub fn message(&self, c: Channel, d: Direction) {
        self.messages[idx(c, d)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn duplicate(&self) {
        self.duplicates.fetch_add(1, Ordering::Relaxed);
    }

    /// One message written to a subscribed session `secs` after the gateway received it.
    pub fn delivered(&self, secs: f64) {
        let secs = secs.max(0.0);
        for (i, b) in BUCKETS.iter().enumerate() {
            if secs <= *b {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.delivery_count.fetch_add(1, Ordering::Relaxed);
        self.delivery_sum_micros.fetch_add((secs * 1e6) as u64, Ordering::Relaxed);
    }

    pub fn render(&self, node: &str, customers: i64, agents: i64, queues: &BTreeMap<String, i64>) -> String {
        let mut o = String::with_capacity(2048);
        let node = node.replace(['\\', '"', '\n'], "_");
        let _ = writeln!(o, "# HELP gateway_sessions_open Open WebSocket sessions on this node.");
        let _ = writeln!(o, "# TYPE gateway_sessions_open gauge");
        let _ = writeln!(o, "gateway_sessions_open{{node=\"{node}\",role=\"customer\"}} {customers}");
        let _ = writeln!(o, "gateway_sessions_open{{node=\"{node}\",role=\"agent\"}} {agents}");
        let _ = writeln!(o, "# HELP gateway_messages_total Canonical messages appended through this node.");
        let _ = writeln!(o, "# TYPE gateway_messages_total counter");
        for (c, d) in [
            (Channel::Whatsapp, Direction::Inbound),
            (Channel::Whatsapp, Direction::Outbound),
            (Channel::Sip, Direction::Inbound),
            (Channel::Sip, Direction::Outbound),
        ] {
            let _ = writeln!(
                o,
                "gateway_messages_total{{node=\"{node}\",channel=\"{}\",direction=\"{}\"}} {}",
                c.as_str(),
                d.as_str(),
                self.messages[idx(c, d)].load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(o, "# HELP gateway_duplicates_total Duplicate ingress or client_ref repeats refused.");
        let _ = writeln!(o, "# TYPE gateway_duplicates_total counter");
        let _ = writeln!(o, "gateway_duplicates_total{{node=\"{node}\"}} {}", self.duplicates.load(Ordering::Relaxed));
        let _ = writeln!(o, "# HELP gateway_queue_depth Conversations waiting for an agent, per skill (cluster-wide).");
        let _ = writeln!(o, "# TYPE gateway_queue_depth gauge");
        for (skill, n) in queues {
            let skill = skill.replace(['\\', '"', '\n'], "_");
            let _ = writeln!(o, "gateway_queue_depth{{skill=\"{skill}\"}} {n}");
        }
        let _ =
            writeln!(o, "# HELP gateway_delivery_seconds Time from the gateway receiving a message to writing it to a subscribed session.");
        let _ = writeln!(o, "# TYPE gateway_delivery_seconds histogram");
        for (i, b) in BUCKETS.iter().enumerate() {
            let _ =
                writeln!(o, "gateway_delivery_seconds_bucket{{node=\"{node}\",le=\"{b}\"}} {}", self.buckets[i].load(Ordering::Relaxed));
        }
        let count = self.delivery_count.load(Ordering::Relaxed);
        let _ = writeln!(o, "gateway_delivery_seconds_bucket{{node=\"{node}\",le=\"+Inf\"}} {count}");
        let _ = writeln!(
            o,
            "gateway_delivery_seconds_sum{{node=\"{node}\"}} {}",
            self.delivery_sum_micros.load(Ordering::Relaxed) as f64 / 1e6
        );
        let _ = writeln!(o, "gateway_delivery_seconds_count{{node=\"{node}\"}} {count}");
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_required_series_with_cumulative_buckets() {
        let m = Metrics::default();
        m.message(Channel::Sip, Direction::Inbound);
        m.delivered(0.004);
        m.delivered(3.0);
        let text = m.render("n1", 5, 2, &BTreeMap::from([("chat".to_string(), 3)]));
        for needle in [
            "gateway_sessions_open{node=\"n1\",role=\"customer\"} 5",
            "gateway_messages_total{node=\"n1\",channel=\"sip\",direction=\"inbound\"} 1",
            "gateway_queue_depth{skill=\"chat\"} 3",
            "gateway_delivery_seconds_bucket{node=\"n1\",le=\"0.005\"} 1",
            "gateway_delivery_seconds_bucket{node=\"n1\",le=\"5\"} 2",
            "gateway_delivery_seconds_count{node=\"n1\"} 2",
        ] {
            assert!(text.contains(needle), "missing {needle} in\n{text}");
        }
    }
}
