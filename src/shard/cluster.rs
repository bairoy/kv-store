//! Where the servers of a cluster are, read from configuration.

use std::collections::HashMap;

use crate::raft::types::NodeId;
use crate::shard::node::ServerAddress;

#[derive(Debug, Clone)]
pub struct ClusterSpec {
    pub servers: HashMap<NodeId, ServerAddress>,
    pub initial: Vec<NodeId>,
    pub bind: ServerAddress,
}

pub fn from_env(id: NodeId) -> Result<ClusterSpec, String> {
    from_lookup(id, &|name| std::env::var(name).ok())
}

pub fn from_lookup(
    id: NodeId,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<ClusterSpec, String> {
    let port = |name: &str| -> Result<Option<u16>, String> {
        lookup(name)
            .map(|text| {
                text.trim()
                    .parse::<u16>()
                    .map_err(|_| format!("{name} must be a port number"))
            })
            .transpose()
    };

    let default_peer = port("SHARD_PEER_PORT")?;
    let default_client = port("SHARD_CLIENT_PORT")?;

    let configured = lookup("SHARD_SERVERS").filter(|text| !text.trim().is_empty());

    let mut servers = HashMap::new();
    let mut order = Vec::new();

    match &configured {
        Some(text) => {
            for entry in text.split(',') {
                let (server, address) = parse_entry(entry, default_peer, default_client)?;

                if servers.insert(server, address).is_some() {
                    return Err(format!("server {server} is listed twice in SHARD_SERVERS"));
                }

                order.push(server);
            }
        }
        None => {
            for server in 1..=5u64 {
                servers.insert(
                    server,
                    ServerAddress {
                        peer: format!("127.0.0.1:{}", 9200 + server),
                        client: format!("127.0.0.1:{}", 7200 + server),
                    },
                );
                order.push(server);
            }
        }
    }

    let own = servers
        .get(&id)
        .ok_or_else(|| format!("server {id} is not in the server list {order:?}"))?;

    let initial = match lookup("SHARD_INITIAL").filter(|text| !text.trim().is_empty()) {
        Some(text) => text
            .split(',')
            .map(|part| {
                part.trim()
                    .parse::<NodeId>()
                    .map_err(|_| format!("SHARD_INITIAL: {part:?} is not a server id"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        None if configured.is_some() => order.clone(),
        None => order.iter().copied().take(4).collect(),
    };

    if let Some(unknown) = initial.iter().find(|server| !servers.contains_key(server)) {
        return Err(format!("SHARD_INITIAL lists unknown server {unknown}"));
    }

    let bind_host = lookup("SHARD_BIND_HOST")
        .filter(|host| !host.trim().is_empty())
        .unwrap_or_else(|| {
            if configured.is_some() {
                "0.0.0.0".to_string()
            } else {
                "127.0.0.1".to_string()
            }
        });

    let bind = ServerAddress {
        peer: format!("{bind_host}:{}", port_of(&own.peer)?),
        client: format!("{bind_host}:{}", port_of(&own.client)?),
    };

    Ok(ClusterSpec {
        servers,
        initial,
        bind,
    })
}

fn parse_entry(
    entry: &str,
    default_peer: Option<u16>,
    default_client: Option<u16>,
) -> Result<(NodeId, ServerAddress), String> {
    let (id, rest) = entry
        .trim()
        .split_once('=')
        .ok_or_else(|| format!("{entry:?}: expected id=host[:peer_port[:client_port]]"))?;

    let id: NodeId = id
        .trim()
        .parse()
        .map_err(|_| format!("{entry:?}: {id:?} is not a server id"))?;

    let mut parts = rest.split(':');
    let host = parts.next().unwrap_or("").trim();

    if host.is_empty() {
        return Err(format!("{entry:?}: missing host"));
    }

    let number = |text: Option<&str>, fallback: Option<u16>, base: u64| -> Result<u16, String> {
        match text {
            Some(text) => text
                .trim()
                .parse()
                .map_err(|_| format!("{entry:?}: {text:?} is not a port")),
            None => Ok(fallback.unwrap_or((base + id) as u16)),
        }
    };

    let peer = number(parts.next(), default_peer, 9200)?;
    let client = number(parts.next(), default_client, 7200)?;

    if parts.next().is_some() {
        return Err(format!(
            "{entry:?}: too many ':' parts (IPv6 is not supported)"
        ));
    }

    Ok((
        id,
        ServerAddress {
            peer: format!("{host}:{peer}"),
            client: format!("{host}:{client}"),
        },
    ))
}

fn port_of(address: &str) -> Result<u16, String> {
    address
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .ok_or_else(|| format!("{address:?} has no port"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(id: NodeId, settings: &[(&str, &str)]) -> Result<ClusterSpec, String> {
        let settings: HashMap<String, String> = settings
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        from_lookup(id, &|name| settings.get(name).cloned())
    }

    #[test]
    fn the_default_is_five_local_servers_with_four_holding_data() {
        let spec = spec(2, &[]).unwrap();

        assert_eq!(spec.servers.len(), 5);
        assert_eq!(spec.servers[&2].peer, "127.0.0.1:9202");
        assert_eq!(spec.servers[&2].client, "127.0.0.1:7202");
        assert_eq!(spec.initial, vec![1, 2, 3, 4]);
        assert_eq!(spec.bind.client, "127.0.0.1:7202");
    }

    #[test]
    fn servers_can_be_named_by_host_with_shared_ports() {
        let spec = spec(
            3,
            &[
                ("SHARD_SERVERS", "1=node1,2=node2,3=node3"),
                ("SHARD_PEER_PORT", "9200"),
                ("SHARD_CLIENT_PORT", "7200"),
            ],
        )
        .unwrap();

        assert_eq!(spec.servers[&1].peer, "node1:9200");
        assert_eq!(spec.servers[&3].client, "node3:7200");
        assert_eq!(spec.initial, vec![1, 2, 3]);
        assert_eq!(spec.bind.peer, "0.0.0.0:9200");
        assert_eq!(spec.bind.client, "0.0.0.0:7200");
    }

    #[test]
    fn an_entry_can_carry_its_own_ports() {
        let spec = spec(1, &[("SHARD_SERVERS", "1=10.0.0.5:9300:7300,2=10.0.0.6")]).unwrap();

        assert_eq!(spec.servers[&1].peer, "10.0.0.5:9300");
        assert_eq!(spec.servers[&1].client, "10.0.0.5:7300");
        assert_eq!(spec.servers[&2].peer, "10.0.0.6:9202");
        assert_eq!(spec.bind.client, "0.0.0.0:7300");
    }

    #[test]
    fn the_bind_host_and_initial_servers_can_be_overridden() {
        let spec = spec(
            1,
            &[
                ("SHARD_SERVERS", "1=a,2=b,3=c,4=d"),
                ("SHARD_INITIAL", "1,2,3"),
                ("SHARD_BIND_HOST", "127.0.0.1"),
            ],
        )
        .unwrap();

        assert_eq!(spec.initial, vec![1, 2, 3]);
        assert!(spec.bind.peer.starts_with("127.0.0.1:"));
    }

    #[test]
    fn mistakes_are_reported_clearly() {
        assert!(spec(9, &[]).unwrap_err().contains("not in the server list"));
        assert!(spec(1, &[("SHARD_SERVERS", "1")]).is_err());
        assert!(spec(1, &[("SHARD_SERVERS", "x=host")]).is_err());
        assert!(spec(1, &[("SHARD_SERVERS", "1=,2=b")]).is_err());
        assert!(spec(1, &[("SHARD_SERVERS", "1=a,1=b")]).is_err());
        assert!(spec(1, &[("SHARD_SERVERS", "1=a:x")]).is_err());
        assert!(spec(1, &[("SHARD_SERVERS", "1=a:1:2:3")]).is_err());
        assert!(spec(1, &[("SHARD_PEER_PORT", "nope")]).is_err());
        assert!(
            spec(1, &[("SHARD_SERVERS", "1=a,2=b"), ("SHARD_INITIAL", "1,7")])
                .unwrap_err()
                .contains("unknown server 7")
        );
    }
}
