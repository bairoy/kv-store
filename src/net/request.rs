//! The V11 client request line: a command, optionally tagged with a request id.

use crate::net::protocol::{Command, parse_request};
use crate::raft::session::valid_client_id;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub client: String,
    pub seq: u64,
}

impl Session {
    pub fn label(&self) -> String {
        format!("{}:{}", self.client, self.seq)
    }

    pub fn txn_id(&self) -> String {
        format!("c-{}-{}", self.client, self.seq)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Get(String),
    Set(String, String),
    Delete(String),
    Incr(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub session: Option<Session>,
    pub op: Op,
}

impl Request {
    pub fn key(&self) -> &str {
        match &self.op {
            Op::Get(key) | Op::Set(key, _) | Op::Delete(key) | Op::Incr(key) => key,
        }
    }

    pub fn is_write(&self) -> bool {
        !matches!(self.op, Op::Get(_))
    }
}

pub fn split_session(line: &str) -> Result<Option<(Session, &str)>, String> {
    let Some(rest) = line.strip_prefix("REQ ") else {
        return Ok(None);
    };

    let mut parts = rest.splitn(3, ' ');

    let (Some(client), Some(seq), Some(command)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err("usage: REQ <client> <seq> <command>".to_string());
    };

    if !valid_client_id(client) {
        return Err("invalid client id (use 1-64 letters, digits, '-', '_' or '.')".to_string());
    }

    let seq: u64 = seq
        .parse()
        .map_err(|_| "the request number must be a positive integer".to_string())?;

    if seq == 0 {
        return Err("the request number must be a positive integer".to_string());
    }

    Ok(Some((
        Session {
            client: client.to_string(),
            seq,
        },
        command,
    )))
}

pub fn parse(line: &str) -> Result<Request, String> {
    let (session, command) = match split_session(line)? {
        Some((session, command)) => (Some(session), command),
        None => (None, line),
    };

    let op = if let Some(key) = command.strip_prefix("INCR ") {
        if key.is_empty() || key.contains(' ') {
            return Err("usage: INCR <key>".to_string());
        }

        Op::Incr(key.to_string())
    } else {
        match parse_request(command) {
            Some(Command::Set(key, value)) => Op::Set(key, value),
            Some(Command::Get(key)) => Op::Get(key),
            Some(Command::Delete(key)) => Op::Delete(key),
            None => return Err("invalid command".to_string()),
        }
    };

    Ok(Request { session, op })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_command_has_no_session() {
        let request = parse("SET a b c").unwrap();

        assert_eq!(request.session, None);
        assert_eq!(request.op, Op::Set("a".into(), "b c".into()));
    }

    #[test]
    fn a_tagged_command_carries_its_identity() {
        let request = parse("REQ c-1 12 INCR hits").unwrap();

        assert_eq!(
            request.session,
            Some(Session {
                client: "c-1".into(),
                seq: 12
            })
        );
        assert_eq!(request.op, Op::Incr("hits".into()));
        assert_eq!(request.key(), "hits");
    }

    #[test]
    fn malformed_tags_are_rejected() {
        assert!(parse("REQ").is_err() || parse("REQ ").is_err());
        assert!(parse("REQ c SET a b").is_err());
        assert!(parse("REQ c x SET a b").is_err());
        assert!(parse("REQ c 0 SET a b").is_err());
        assert!(parse("REQ bad!id 1 SET a b").is_err());
        assert!(parse("REQ c 1 FROB a").is_err());
        assert!(parse("INCR").is_err());
    }

    #[test]
    fn the_transaction_id_is_derived_from_the_tag() {
        let (session, rest) = split_session("REQ c 3 TXN SET a 1").unwrap().unwrap();

        assert_eq!(rest, "TXN SET a 1");
        assert_eq!(session.txn_id(), "c-c-3");
    }
}
