//! Operations as machines the host drives.
//!
//! The core never touches a store. An operation is ordinary sequential
//! code, an `async fn` that awaits [`Io`] for what it needs from the
//! host; [`Machine`] polls it, and each time it waits, hands the host
//! the [`Request`] it is waiting on. The host performs the request and
//! resumes the machine with the [`Reply`], until the operation is done.
//!
//! ```text
//! let mut step = machine.resume(None);
//! while let Step::Request(request) = step {
//!     let reply = perform(request);
//!     step = machine.resume_at(wall_clock(), Some(reply));
//! }
//! ```
//!
//! No threads, no runtime and no waker are involved: a machine only
//! makes progress inside `resume`, so the same code runs under a
//! blocking host and an asynchronous one, and in WebAssembly.
//!
//! A host that can't perform a request (its store raised, its merge
//! function raised something it doesn't file as a conflict) replies
//! [`Reply::Failed`], keeping the reason. The operation unwinds, undoing
//! what it can, and finishes with an error the host maps back to the
//! reason it kept.

use crate::merge::{MergeAnswer, MergeAsk};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// What an operation needs from the host before it can go on.
#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    /// The values of these keys; a key the store lacks is left out of
    /// the reply.
    Get(Vec<String>),
    /// Every stored key starting with this prefix, in any order.
    /// Answered [`Reply::Keys`].
    Scan(String),
    /// One atomic write, landing only if every expected key holds what
    /// is expected (`None`: absent). Answered [`Reply::Landed`].
    Cas(Cas),
    /// Sleep this many seconds, then answer [`Reply::Waited`].
    Wait(f64),
    /// Run these merge functions, in order; answered [`Reply::Merged`]
    /// with one answer each.
    Merge(Vec<MergeAsk>),
    /// A last-resort guess at a branch's head, for a head that is
    /// present but unusable and has no usable backup. Answered
    /// [`Reply::Recovered`].
    Recover(String),
    /// The time to stamp a commit with, in seconds, from the clock the
    /// caller may replace. Answered [`Reply::Now`].
    Now,
    /// Part of the operation's answer, ready before the rest: the host
    /// hands it on, to a caller iterating over the answer, and answers
    /// [`Reply::Next`] when it wants more. A host that wants no more
    /// drops the machine instead.
    Emit(crate::json::Value),
}

/// A conditional batch: `writes` and `removes` land together, and only
/// if every key in `expect` holds the bytes given (`None`: absent).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Cas {
    pub expect: Vec<(String, Option<Vec<u8>>)>,
    pub writes: Vec<(String, Write)>,
    pub removes: Vec<String>,
}

/// What to write under a key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Write {
    Bytes(Vec<u8>),
    /// The bytes the host holds for this key: a value's blob or a
    /// chunk it handed the core by key, which the core never copies.
    Held,
}

/// The host's answer to a [`Request`].
#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    Values(HashMap<String, Vec<u8>>),
    Keys(Vec<String>),
    Landed(bool),
    Waited,
    Merged(Vec<MergeAnswer>),
    Recovered(Option<String>),
    Now(f64),
    Next,
    /// The host couldn't perform the request, and holds the reason.
    Failed,
}

/// The host failed a request; it holds the reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostFailed;

/// Where a machine stands after a `resume`.
#[derive(Debug, PartialEq)]
pub enum Step<T> {
    Request(Request),
    Done(T),
}

#[derive(Default)]
struct Channel {
    request: Option<Request>,
    reply: Option<Reply>,
    wall: Option<f64>,
}

/// An operation's line to its host.
#[derive(Clone, Default)]
pub struct Io {
    channel: Rc<RefCell<Channel>>,
}

impl Io {
    /// The stored values of `keys`, keyed by key; absent keys are left
    /// out. Keys are asked for in the order given.
    pub async fn get(&self, keys: Vec<String>) -> Result<HashMap<String, Vec<u8>>, HostFailed> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        match self.ask(Request::Get(keys)).await {
            Reply::Values(values) => Ok(values),
            Reply::Failed => Err(HostFailed),
            other => panic!("a read answered with {other:?}"),
        }
    }

    /// The stored value of one key.
    pub async fn get_one(&self, key: &str) -> Result<Option<Vec<u8>>, HostFailed> {
        Ok(self.get(vec![key.to_string()]).await?.remove(key))
    }

    /// Every stored key starting with `prefix`, sorted.
    pub async fn scan(&self, prefix: &str) -> Result<Vec<String>, HostFailed> {
        match self.ask(Request::Scan(prefix.to_string())).await {
            Reply::Keys(mut keys) => {
                // A host's listing may include keys outside the prefix
                // (a backend that matches loosely) or repeat one.
                keys.retain(|k| k.starts_with(prefix));
                keys.sort();
                keys.dedup();
                Ok(keys)
            }
            Reply::Failed => Err(HostFailed),
            other => panic!("a scan answered with {other:?}"),
        }
    }

    /// Whether the batch landed.
    pub async fn cas(&self, cas: Cas) -> Result<bool, HostFailed> {
        match self.ask(Request::Cas(cas)).await {
            Reply::Landed(landed) => Ok(landed),
            Reply::Failed => Err(HostFailed),
            other => panic!("a write answered with {other:?}"),
        }
    }

    pub async fn wait(&self, seconds: f64) -> Result<(), HostFailed> {
        match self.ask(Request::Wait(seconds)).await {
            Reply::Waited => Ok(()),
            Reply::Failed => Err(HostFailed),
            other => panic!("a wait answered with {other:?}"),
        }
    }

    pub async fn merge(&self, asks: Vec<MergeAsk>) -> Result<Vec<MergeAnswer>, HostFailed> {
        if asks.is_empty() {
            return Ok(vec![]);
        }
        let n = asks.len();
        match self.ask(Request::Merge(asks)).await {
            Reply::Merged(answers) if answers.len() == n => Ok(answers),
            Reply::Failed => Err(HostFailed),
            other => panic!("{n} merges answered with {other:?}"),
        }
    }

    pub async fn recover(&self, branch: &str) -> Result<Option<String>, HostFailed> {
        match self.ask(Request::Recover(branch.to_string())).await {
            Reply::Recovered(commit) => Ok(commit),
            Reply::Failed => Err(HostFailed),
            other => panic!("a recovery answered with {other:?}"),
        }
    }

    /// Hand the host part of the answer, and wait until it wants more.
    pub async fn emit(&self, part: crate::json::Value) -> Result<(), HostFailed> {
        match self.ask(Request::Emit(part)).await {
            Reply::Next => Ok(()),
            Reply::Failed => Err(HostFailed),
            other => panic!("an emit answered with {other:?}"),
        }
    }

    /// The commit clock's time.
    pub async fn now(&self) -> Result<f64, HostFailed> {
        match self.ask(Request::Now).await {
            Reply::Now(now) => Ok(now),
            Reply::Failed => Err(HostFailed),
            other => panic!("the time answered with {other:?}"),
        }
    }

    /// Wall time, in seconds, as of the host's latest reply: what lease
    /// and in-flight expiries are measured by.
    ///
    /// # Panics
    ///
    /// When the host never gave the time: a mistake in its driver.
    pub fn wall(&self) -> f64 {
        self.channel
            .borrow()
            .wall
            .expect("the host gives wall time with every reply")
    }

    async fn ask(&self, request: Request) -> Reply {
        Exchange {
            io: self,
            request: Some(request),
        }
        .await
    }
}

/// One request and its reply: pending until the host has answered.
struct Exchange<'a> {
    io: &'a Io,
    request: Option<Request>,
}

impl Future for Exchange<'_> {
    type Output = Reply;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Reply> {
        let mut channel = self.io.channel.borrow_mut();
        if let Some(request) = self.request.take() {
            channel.request = Some(request);
            return Poll::Pending;
        }
        match channel.reply.take() {
            Some(reply) => Poll::Ready(reply),
            None => Poll::Pending,
        }
    }
}

/// An operation in progress.
pub struct Machine<T> {
    io: Io,
    future: Pin<Box<dyn Future<Output = T>>>,
    waiting: bool,
    done: bool,
}

impl<T: 'static> Machine<T> {
    /// A machine running `op`, which is handed the [`Io`] it awaits.
    pub fn new<F, Fut>(op: F) -> Self
    where
        F: FnOnce(Io) -> Fut,
        Fut: Future<Output = T> + 'static,
    {
        let io = Io::default();
        let future = Box::pin(op(io.clone()));
        Machine {
            io,
            future,
            waiting: false,
            done: false,
        }
    }

    /// Run until the operation needs the host or is done. The first
    /// call passes no reply; each later one answers the request the
    /// call before it returned.
    ///
    /// # Panics
    ///
    /// When resumed after it is done, or without a reply it is
    /// waiting for: both are mistakes in the host's driver.
    pub fn resume(&mut self, reply: Option<Reply>) -> Step<T> {
        assert!(!self.done, "resumed a machine that is done");
        match (self.waiting, reply.is_some()) {
            (true, false) => panic!("resumed without answering the request"),
            (false, true) => panic!("resumed with a reply to no request"),
            _ => {}
        }
        self.io.channel.borrow_mut().reply = reply;
        let mut cx = Context::from_waker(Waker::noop());
        match self.future.as_mut().poll(&mut cx) {
            Poll::Ready(out) => {
                self.waiting = false;
                self.done = true;
                Step::Done(out)
            }
            Poll::Pending => {
                let request = self.io.channel.borrow_mut().request.take();
                self.waiting = true;
                Step::Request(request.expect("an operation waits only on its host"))
            }
        }
    }

    /// [`resume`](Self::resume), telling the wall time first. An
    /// operation that reads it needs it with every resume.
    pub fn resume_at(&mut self, wall: f64, reply: Option<Reply>) -> Step<T> {
        self.io.channel.borrow_mut().wall = Some(wall);
        self.resume(reply)
    }

    /// Drive the machine to the end against `get`, a store read. For
    /// tests, and for hosts whose store is at hand, running operations
    /// that only read.
    pub fn run(mut self, mut get: impl FnMut(&[String]) -> HashMap<String, Vec<u8>>) -> T {
        let mut step = self.resume(None);
        loop {
            match step {
                Step::Done(out) => return out,
                Step::Request(Request::Get(keys)) => {
                    step = self.resume(Some(Reply::Values(get(&keys))));
                }
                Step::Request(other) => panic!("a read-only run was asked for {other:?}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_operation_asks_and_is_answered() {
        let mut machine = Machine::new(|io| async move {
            let a = io.get(vec!["a".into(), "b".into()]).await?;
            let b = io.get(vec!["c".into()]).await?;
            Ok::<_, HostFailed>((a.len(), b.get("c").cloned()))
        });
        assert_eq!(
            machine.resume(None),
            Step::Request(Request::Get(vec!["a".into(), "b".into()]))
        );
        let reply = HashMap::from([("a".to_string(), b"1".to_vec())]);
        assert_eq!(
            machine.resume(Some(Reply::Values(reply))),
            Step::Request(Request::Get(vec!["c".into()]))
        );
        let reply = HashMap::from([("c".to_string(), b"3".to_vec())]);
        assert_eq!(
            machine.resume(Some(Reply::Values(reply))),
            Step::Done(Ok((1, Some(b"3".to_vec()))))
        );
    }

    #[test]
    fn an_empty_read_never_reaches_the_host() {
        let machine = Machine::new(|io| async move { io.get(vec![]).await.map(|v| v.len()) });
        assert_eq!(machine.run(|_| panic!("asked for nothing")), Ok(0));
    }

    #[test]
    fn a_failed_request_unwinds_the_operation() {
        let mut machine = Machine::new(|io| async move {
            let landed = io.cas(Cas::default()).await;
            let after = io.get(vec!["undo".into()]).await.map(|v| v.len());
            (landed, after)
        });
        assert!(matches!(
            machine.resume(None),
            Step::Request(Request::Cas(_))
        ));
        assert!(matches!(
            machine.resume(Some(Reply::Failed)),
            Step::Request(Request::Get(_))
        ));
        assert_eq!(
            machine.resume(Some(Reply::Values(HashMap::new()))),
            Step::Done((Err(HostFailed), Ok(0)))
        );
    }

    #[test]
    fn wall_time_is_the_latest_reply_s() {
        let mut machine = Machine::new(|io| async move {
            let before = io.wall();
            io.wait(1.0).await.unwrap();
            (before, io.wall(), io.now().await.unwrap())
        });
        assert_eq!(
            machine.resume_at(5.0, None),
            Step::Request(Request::Wait(1.0))
        );
        assert_eq!(
            machine.resume_at(6.0, Some(Reply::Waited)),
            Step::Request(Request::Now)
        );
        assert_eq!(
            machine.resume_at(6.0, Some(Reply::Now(9.5))),
            Step::Done((5.0, 6.0, 9.5))
        );
    }

    #[test]
    #[should_panic(expected = "without answering")]
    fn resuming_without_a_reply_is_a_driver_bug() {
        let mut machine = Machine::new(|io| async move { io.get(vec!["a".into()]).await });
        let _ = machine.resume(None);
        let _ = machine.resume(None);
    }
}
