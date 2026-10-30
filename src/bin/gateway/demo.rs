//! Demo mode: run the four cluster servers as child processes, and let
//! visitors crash one at a time (it restarts by itself).

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const RESTART_AFTER: Duration = Duration::from_secs(8);
const CRASH_RESTART_AFTER: Duration = Duration::from_secs(2);
const COOLDOWN: Duration = Duration::from_secs(6);
const KEPT_EVENTS: usize = 12;

#[derive(Default)]
struct Inner {
    children: HashMap<u64, Child>,
    restart_at: HashMap<u64, Instant>,
    last_fault: Option<Instant>,
}

pub struct Demo {
    binary: PathBuf,
    dir: PathBuf,
    ids: Vec<u64>,
    inner: Mutex<Inner>,
    events: Mutex<VecDeque<(Instant, String)>>,
}

impl Demo {
    pub fn start(binary: PathBuf, dir: PathBuf, ids: Vec<u64>) -> std::io::Result<Self> {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;

        let demo = Self {
            binary,
            dir,
            ids,
            inner: Mutex::new(Inner::default()),
            events: Mutex::new(VecDeque::new()),
        };

        {
            let mut inner = demo.lock();

            for id in &demo.ids {
                let child = demo.spawn(*id)?;
                inner.children.insert(*id, child);
            }
        }

        demo.event("cluster started: 4 servers, 8 partitions, replication 3".to_string());

        Ok(demo)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn spawn(&self, id: u64) -> std::io::Result<Child> {
        Command::new(&self.binary)
            .arg(id.to_string())
            .current_dir(&self.dir)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
    }

    fn event(&self, text: String) {
        let mut events = self.events.lock().unwrap_or_else(|e| e.into_inner());

        events.push_back((Instant::now(), text));

        while events.len() > KEPT_EVENTS {
            events.pop_front();
        }
    }

    pub fn events(&self) -> Vec<(u64, String)> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .rev()
            .map(|(when, text)| (when.elapsed().as_secs(), text.clone()))
            .collect()
    }

    /// Restart servers that are due or that died on their own.
    pub fn tick(&self) {
        let mut messages = Vec::new();

        {
            let mut inner = self.lock();
            let now = Instant::now();

            for id in &self.ids {
                let exited = inner
                    .children
                    .get_mut(id)
                    .is_some_and(|child| matches!(child.try_wait(), Ok(Some(_))));

                if exited {
                    inner.children.remove(id);
                    inner.restart_at.insert(*id, now + CRASH_RESTART_AFTER);
                    messages.push(format!("server {id} exited unexpectedly"));
                }

                let due = inner.restart_at.get(id).is_some_and(|at| *at <= now);

                if due && !inner.children.contains_key(id) {
                    match self.spawn(*id) {
                        Ok(child) => {
                            inner.children.insert(*id, child);
                            inner.restart_at.remove(id);
                            messages.push(format!(
                                "server {id} restarted; it catches up from its peers"
                            ));
                        }
                        Err(error) => {
                            inner.restart_at.insert(*id, now + CRASH_RESTART_AFTER);
                            messages.push(format!("server {id} could not restart: {error}"));
                        }
                    }
                }
            }
        }

        for message in messages {
            self.event(message);
        }
    }

    pub fn is_running(&self, id: u64) -> bool {
        self.lock().children.contains_key(&id)
    }

    /// `kill -9` one server. Only one fault at a time, with a cooldown, so
    /// every partition keeps its majority.
    pub fn kill(&self, id: u64) -> Result<String, String> {
        let mut inner = self.lock();

        if !self.ids.contains(&id) {
            return Err("no such server".to_string());
        }

        if inner.children.len() < self.ids.len() || !inner.restart_at.is_empty() {
            return Err("a server is already down; wait for it to come back".to_string());
        }

        if inner.last_fault.is_some_and(|at| at.elapsed() < COOLDOWN) {
            return Err("please wait a few seconds between crashes".to_string());
        }

        if let Some(mut child) = inner.children.remove(&id) {
            let _ = child.kill();
            let _ = child.wait();
        }

        inner.restart_at.insert(id, Instant::now() + RESTART_AFTER);
        inner.last_fault = Some(Instant::now());
        drop(inner);

        let message = format!(
            "server {id} was killed (kill -9); it restarts in {} s",
            RESTART_AFTER.as_secs()
        );
        self.event(message.clone());

        Ok(message)
    }

    pub fn shutdown(&self) {
        let mut inner = self.lock();

        for (_, mut child) in inner.children.drain() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Demo {
    fn drop(&mut self) {
        self.shutdown();
    }
}
