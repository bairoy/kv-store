//! The client against scripted servers.

use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::*;
use crate::net::framing::{FrameDecoder, encode_frame};

enum Action {
    Reply(&'static str),
    Drop,
}

struct FakeServer {
    address: String,
    seen: Arc<Mutex<Vec<String>>>,
}

impl FakeServer {
    fn start(script: impl Fn(usize, &str) -> Action + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_in_thread = Arc::clone(&seen);

        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                serve(stream, &script, &seen_in_thread);
            }
        });

        Self { address, seen }
    }

    fn requests(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

fn serve(
    mut stream: TcpStream,
    script: &(impl Fn(usize, &str) -> Action + Send + 'static),
    seen: &Arc<Mutex<Vec<String>>>,
) {
    let mut decoder = FrameDecoder::new();

    loop {
        let frame = loop {
            match decoder.next_frame() {
                Ok(Some(frame)) => break frame,
                Ok(None) => match decoder.read_from(&mut stream) {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                },
                Err(_) => return,
            }
        };

        let request = String::from_utf8(frame).unwrap();
        let number = {
            let mut seen = seen.lock().unwrap();
            seen.push(request.clone());
            seen.len() - 1
        };

        match script(number, &request) {
            Action::Reply(text) => {
                if stream
                    .write_all(&encode_frame(text.as_bytes()).unwrap())
                    .is_err()
                {
                    return;
                }
            }
            Action::Drop => return,
        }
    }
}

fn quick() -> ClientOptions {
    ClientOptions {
        request_timeout: Duration::from_millis(500),
        max_attempts: 6,
        deadline: Duration::from_secs(5),
        base_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
        ..ClientOptions::default()
    }
}

fn dead_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

#[test]
fn replies_are_decoded() {
    assert_eq!(parse_reply("+OK"), Reply::Ok);
    assert_eq!(parse_reply("$-1\r\n"), Reply::NotFound);
    assert_eq!(parse_reply("$5\r\nhello\r\n"), Reply::Value("hello".into()));
    assert_eq!(parse_reply("$0\r\n\r\n"), Reply::Value(String::new()));
    assert_eq!(parse_reply("-ERR BUSY: x"), Reply::Error("BUSY: x".into()));
}

#[test]
fn errors_are_classified_by_whether_the_write_could_have_happened() {
    for message in [
        "BUSY: too many writes in flight; retry shortly",
        "not leader; leader is node 2",
        "not leader; no leader elected yet",
        "STALE_EPOCH: sender is at epoch 3",
        "partition 2: no leader elected yet, retry shortly",
    ] {
        assert_eq!(classify_error(message), Failure::NotApplied, "{message}");
    }

    for message in [
        "timed out waiting for a majority",
        "leadership changed before the write committed; outcome unknown",
        "partition 3 unavailable (node 2: timed out)",
    ] {
        assert_eq!(classify_error(message), Failure::Unknown, "{message}");
    }

    assert_eq!(classify_error("invalid command"), Failure::Permanent);
    assert_eq!(
        classify_error("that key is reserved for internal use"),
        Failure::Permanent
    );
    assert_eq!(
        classify_error("transaction aborted: partition 1 voted NO"),
        Failure::Aborted
    );
}

#[test]
fn backoff_grows_then_is_capped() {
    let options = ClientOptions {
        base_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_secs(1),
        ..ClientOptions::default()
    };

    assert_eq!(backoff_ceiling(&options, 0), Duration::from_millis(20));
    assert_eq!(backoff_ceiling(&options, 1), Duration::from_millis(40));
    assert_eq!(backoff_ceiling(&options, 3), Duration::from_millis(160));
    assert_eq!(backoff_ceiling(&options, 10), Duration::from_secs(1));
    assert_eq!(backoff_ceiling(&options, 1000), Duration::from_secs(1));
}

#[test]
fn jitter_stays_within_the_ceiling_and_actually_varies() {
    let mut client = Client::with_options(vec!["127.0.0.1:1".into()], quick());
    let ceiling = Duration::from_millis(100);

    let samples: Vec<Duration> = (0..200).map(|_| client.jitter(ceiling)).collect();

    assert!(samples.iter().all(|sample| *sample <= ceiling));
    assert!(
        samples
            .iter()
            .any(|sample| *sample < Duration::from_millis(50))
    );
    assert!(
        samples
            .iter()
            .any(|sample| *sample > Duration::from_millis(50))
    );
}

#[test]
fn a_write_is_tagged_and_the_number_advances_per_request() {
    let server = FakeServer::start(|_, _| Action::Reply("+OK"));
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    client.set("a", "1").unwrap();
    client.set("b", "2").unwrap();

    let id = client.id().to_string();

    assert_eq!(
        server.requests(),
        vec![format!("REQ {id} 1 SET a 1"), format!("REQ {id} 2 SET b 2")]
    );
}

#[test]
fn busy_is_retried_with_the_same_request_number() {
    let server = FakeServer::start(|n, _| {
        if n < 2 {
            Action::Reply("-ERR BUSY: too many writes in flight; retry shortly")
        } else {
            Action::Reply("+OK")
        }
    });
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    client.set("k", "v").unwrap();

    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|request| request == &requests[0]));
    assert_eq!(client.stats().retries, 2);
}

#[test]
fn a_lost_reply_is_retried_with_the_same_id_so_it_applies_once() {
    let server = FakeServer::start(|n, _| {
        if n == 0 {
            Action::Drop
        } else {
            Action::Reply("$1\r\n7\r\n")
        }
    });
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    assert_eq!(client.incr("hits").unwrap(), 7);

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0], requests[1],
        "the retry must reuse the request id"
    );
    assert_eq!(client.stats().ambiguous_failures, 1);
}

#[test]
fn without_request_ids_an_ambiguous_failure_is_not_retried() {
    let server = FakeServer::start(|_, _| Action::Drop);
    let options = ClientOptions {
        idempotent_writes: false,
        ..quick()
    };
    let mut client = Client::with_options(vec![server.address.clone()], options);

    let error = client.set("k", "v").unwrap_err();

    assert!(matches!(error, ClientError::Ambiguous(_)), "{error}");
    assert_eq!(server.requests(), vec!["SET k v".to_string()]);
}

#[test]
fn reads_are_retried_even_without_request_ids() {
    let server = FakeServer::start(|n, _| {
        if n == 0 {
            Action::Drop
        } else {
            Action::Reply("$2\r\nhi\r\n")
        }
    });
    let options = ClientOptions {
        idempotent_writes: false,
        ..quick()
    };
    let mut client = Client::with_options(vec![server.address.clone()], options);

    assert_eq!(client.get("k").unwrap(), Some("hi".to_string()));
    assert_eq!(server.requests().len(), 2);
}

#[test]
fn an_unreachable_server_is_skipped() {
    let alive = FakeServer::start(|_, _| Action::Reply("+OK"));
    let mut client = Client::with_options(vec![dead_address(), alive.address.clone()], quick());

    client.set("k", "v").unwrap();

    assert_eq!(alive.requests().len(), 1);
}

#[test]
fn not_leader_redirects_straight_to_the_named_leader() {
    let leader = FakeServer::start(|_, _| Action::Reply("+OK"));

    let leader_address = leader.address.clone();
    let follower = FakeServer::start(|_, _| Action::Reply("-ERR not leader; leader is node 2"));

    let mut options = quick();
    options.leader_addresses.insert(1, follower.address.clone());
    options.leader_addresses.insert(2, leader_address);

    let mut client = Client::with_options(
        vec![follower.address.clone(), leader.address.clone()],
        options,
    );

    client.set("k", "v").unwrap();

    assert_eq!(follower.requests().len(), 1);
    assert_eq!(leader.requests().len(), 1);
}

#[test]
fn a_permanent_error_is_not_retried() {
    let server = FakeServer::start(|_, _| Action::Reply("-ERR invalid command"));
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    let error = client.set("k", "v").unwrap_err();

    assert_eq!(error, ClientError::Rejected("invalid command".into()));
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn an_aborted_transaction_is_reported_not_retried() {
    let server =
        FakeServer::start(|_, _| Action::Reply("-ERR transaction aborted: partition 1 voted NO"));
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    let error = client.txn(&[("a", Some("1")), ("b", None)]).unwrap_err();

    assert!(matches!(error, ClientError::Aborted(_)));
    assert_eq!(server.requests().len(), 1);
    assert!(server.requests()[0].contains("TXN SET a 1 ; DELETE b"));
}

#[test]
fn the_retry_budget_ends_in_unavailable_and_says_if_the_write_may_have_happened() {
    let server = FakeServer::start(|_, _| Action::Drop);
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    match client.set("k", "v").unwrap_err() {
        ClientError::Unavailable {
            attempts,
            maybe_applied,
            ..
        } => {
            assert_eq!(attempts, 6);
            assert!(maybe_applied);
        }
        other => panic!("{other}"),
    }

    let nobody = dead_address();
    let mut client = Client::with_options(vec![nobody], quick());

    match client.set("k", "v").unwrap_err() {
        ClientError::Unavailable { maybe_applied, .. } => assert!(!maybe_applied),
        other => panic!("{other}"),
    }
}

#[test]
fn the_deadline_stops_retrying_early() {
    let server = FakeServer::start(|_, _| Action::Reply("-ERR BUSY: x"));
    let options = ClientOptions {
        max_attempts: 1000,
        deadline: Duration::from_millis(200),
        base_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(20),
        ..quick()
    };
    let mut client = Client::with_options(vec![server.address.clone()], options);

    let started = std::time::Instant::now();
    assert!(client.set("k", "v").is_err());

    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(client.stats().attempts < 1000);
}

#[test]
fn a_connection_that_failed_is_not_reused() {
    let server = FakeServer::start(|n, _| {
        if n == 0 {
            Action::Drop
        } else {
            Action::Reply("+OK")
        }
    });
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    client.set("a", "1").unwrap();
    client.set("b", "2").unwrap();

    assert_eq!(client.connections.len(), 1);
}

#[test]
fn admin_commands_are_sent_once_and_untagged() {
    let server = FakeServer::start(|_, _| Action::Reply("$2\r\nok\r\n"));
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    assert_eq!(
        client.execute_line("STATUS").unwrap(),
        Reply::Value("ok".into())
    );
    assert_eq!(server.requests(), vec!["STATUS".to_string()]);
}

#[test]
fn a_line_already_tagged_by_the_caller_is_still_retried_as_a_write() {
    let server = FakeServer::start(|n, _| {
        if n == 0 {
            Action::Reply("-ERR BUSY: x")
        } else {
            Action::Reply("+OK")
        }
    });
    let mut client = Client::with_options(vec![server.address.clone()], quick());

    assert_eq!(
        client.execute_line("REQ mine 4 SET k v").unwrap(),
        Reply::Ok
    );
    assert_eq!(server.requests().len(), 2);
    assert!(server.requests().iter().all(|r| r == "REQ mine 4 SET k v"));
}

#[test]
fn admin_commands_wait_longer_than_ordinary_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();

    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = FrameDecoder::new();

        loop {
            if decoder.next_frame().unwrap().is_some() {
                break;
            }
            if decoder.read_from(&mut stream).unwrap() == 0 {
                return;
            }
        }

        thread::sleep(Duration::from_millis(900));
        let _ = stream.write_all(&encode_frame(b"$4\r\ndone\r\n").unwrap());
    });

    let options = ClientOptions {
        request_timeout: Duration::from_millis(300),
        admin_timeout: Duration::from_secs(5),
        ..quick()
    };
    let mut client = Client::with_options(vec![address], options);

    assert_eq!(
        client.execute_line("REBALANCE 1,2").unwrap(),
        Reply::Value("done".into())
    );
}
