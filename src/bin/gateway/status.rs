//! Turning server replies and metrics text into what the web page shows.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub struct PartitionRole {
    pub partition: u32,
    pub role: String,
    pub leader: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ServerStatus {
    pub epoch: u64,
    pub partitions_total: u32,
    pub hosted: Vec<PartitionRole>,
}

pub fn parse_status(text: &str) -> Option<ServerStatus> {
    let mut lines = text.lines();
    let header = lines.next()?;

    let mut epoch = None;
    let mut partitions_total = None;

    for part in header.split(" | ") {
        let part = part.trim();

        if let Some(value) = part.strip_prefix("epoch ") {
            epoch = value.parse().ok();
        } else if let Some(value) = part.strip_suffix(" partitions total") {
            partitions_total = value.parse().ok();
        }
    }

    let hosted = lines
        .filter_map(|line| {
            let (name, rest) = line.trim().split_once(':')?;
            let partition = name.strip_prefix('p')?.parse().ok()?;
            let mut words = rest.split_whitespace();
            let role = words.next()?.to_string();
            let leader = words
                .find_map(|word| word.strip_prefix("leader="))
                .and_then(|value| value.parse().ok());

            Some(PartitionRole {
                partition,
                role,
                leader,
            })
        })
        .collect();

    Some(ServerStatus {
        epoch: epoch?,
        partitions_total: partitions_total?,
        hosted,
    })
}

/// One letter per server for one partition: L leader, F follower, C candidate,
/// `-` not a replica, `x` server down.
pub fn cell(status: Option<&ServerStatus>, partition: u32) -> &'static str {
    let Some(status) = status else { return "x" };

    match status
        .hosted
        .iter()
        .find(|role| role.partition == partition)
    {
        Some(role) => match role.role.as_str() {
            "Leader" => "L",
            "Follower" => "F",
            _ => "C",
        },
        None => "-",
    }
}

pub fn grid(
    statuses: &BTreeMap<u64, Option<ServerStatus>>,
    partitions: u32,
) -> Vec<serde_json::Value> {
    (0..partitions)
        .map(|partition| {
            let cells: BTreeMap<String, &str> = statuses
                .iter()
                .map(|(id, status)| (id.to_string(), cell(status.as_ref(), partition)))
                .collect();

            let leader = statuses.iter().find_map(|(id, status)| {
                let role = status
                    .as_ref()?
                    .hosted
                    .iter()
                    .find(|role| role.partition == partition)?;

                (role.role == "Leader").then_some(*id)
            });

            serde_json::json!({ "partition": partition, "leader": leader, "cells": cells })
        })
        .collect()
}

pub fn http_get(address: &str) -> Option<String> {
    let target = address.to_socket_addrs().ok()?.next()?;
    let mut stream = TcpStream::connect_timeout(&target, Duration::from_millis(400)).ok()?;

    stream
        .set_read_timeout(Some(Duration::from_millis(600)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_millis(400)))
        .ok()?;
    stream.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").ok()?;

    let mut text = String::new();
    stream.read_to_string(&mut text).ok()?;

    text.split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
}

/// Sum of every series called `name` whose labels contain `label` (if given).
pub fn metric_sum(text: &str, name: &str, label: Option<&str>) -> f64 {
    text.lines()
        .filter(|line| !line.starts_with('#'))
        .filter(|line| {
            line.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
        })
        .filter(|line| label.is_none_or(|label| line.contains(label)))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .fold(0.0, |total, part| total + part)
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUS: &str = "server 1 | epoch 7 | 8 partitions total | hosting 3\n  p0: Follower  leader=2 voters=[1, 2, 3] learners=[]\n  p1: Leader    leader=1 voters=[1, 3, 4] learners=[]\n  p5: Candidate leader=? voters=[1, 2, 4] learners=[]";

    #[test]
    fn a_status_reply_is_parsed() {
        let status = parse_status(STATUS).unwrap();

        assert_eq!(status.epoch, 7);
        assert_eq!(status.partitions_total, 8);
        assert_eq!(status.hosted.len(), 3);
        assert_eq!(
            status.hosted[0],
            PartitionRole {
                partition: 0,
                role: "Follower".into(),
                leader: Some(2)
            }
        );
        assert_eq!(status.hosted[2].leader, None);
    }

    #[test]
    fn garbage_is_not_a_status() {
        assert!(parse_status("").is_none());
        assert!(parse_status("-ERR nope").is_none());
    }

    #[test]
    fn cells_show_role_absence_and_downtime() {
        let status = parse_status(STATUS).unwrap();

        assert_eq!(cell(Some(&status), 0), "F");
        assert_eq!(cell(Some(&status), 1), "L");
        assert_eq!(cell(Some(&status), 5), "C");
        assert_eq!(cell(Some(&status), 3), "-");
        assert_eq!(cell(None, 0), "x");
    }

    #[test]
    fn the_grid_names_each_partitions_leader() {
        let mut statuses = BTreeMap::new();
        statuses.insert(1, parse_status(STATUS));
        statuses.insert(2, None);

        let grid = grid(&statuses, 2);

        assert_eq!(grid[1]["leader"], 1);
        assert_eq!(grid[0]["leader"], serde_json::Value::Null);
        assert_eq!(grid[1]["cells"]["2"], "x");
    }

    #[test]
    fn metric_series_are_summed() {
        let text = "# TYPE a counter\nraft_elections_total{group=\"p0\"} 2\nraft_elections_total{group=\"p1\"} 3\nrequests_total{outcome=\"ok\"} 10\nrequests_total{outcome=\"busy\"} 1\nother 5\n";

        assert_eq!(metric_sum(text, "raft_elections_total", None), 5.0);
        assert_eq!(
            metric_sum(text, "requests_total", Some("outcome=\"ok\"")),
            10.0
        );
        assert_eq!(metric_sum(text, "missing", None), 0.0);
        assert_eq!(metric_sum(text, "raft_elections", None), 0.0);
    }
}
