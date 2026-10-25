//! Operations as machines the host drives.
//!
//! The core never touches a store. An operation is ordinary sequential
//! code, an `async fn` that awaits [`Io`] for what it needs from the
//! store; [`Machine`] polls it, and each time it waits, hands the host
//! the [`Request`] it is waiting on. The host performs the request and
//! resumes the machine with the [`Reply`], until the operation is done.
//!
//! ```text
//! let mut step = machine.resume(None);
//! while let Step::Request(Request::Get(keys)) = step {
//!     step = machine.resume(Some(Reply::Values(store.get_many(keys))));
//! }
//! ```
//!
//! No threads, no runtime and no waker are involved: a machine only
//! makes progress inside `resume`, so the same code runs under a
//! blocking host and an asynchronous one, and in WebAssembly.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// What an operation needs from the store before it can go on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// The values of these keys; a key the store lacks is left out of
    /// the reply.
    Get(Vec<String>),
}

/// The host's answer to a [`Request`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Values(HashMap<String, Vec<u8>>),
}

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
}

/// An operation's line to its host.
#[derive(Clone, Default)]
pub struct Io {
    channel: Rc<RefCell<Channel>>,
}

impl Io {
    /// The stored values of `keys`, keyed by key; absent keys are left
    /// out. Keys are asked for in the order given.
    pub async fn get(&self, keys: Vec<String>) -> HashMap<String, Vec<u8>> {
        if keys.is_empty() {
            return HashMap::new();
        }
        match (Exchange {
            io: self,
            request: Some(Request::Get(keys)),
        })
        .await
        {
            Reply::Values(values) => values,
        }
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

    /// Run until the operation needs the store or is done. The first
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
                Step::Request(request.expect("an operation waits only on the store"))
            }
        }
    }

    /// Drive the machine to the end against `get`, a store read. For
    /// tests, and for hosts whose store is at hand.
    pub fn run(mut self, mut get: impl FnMut(&[String]) -> HashMap<String, Vec<u8>>) -> T {
        let mut step = self.resume(None);
        loop {
            match step {
                Step::Done(out) => return out,
                Step::Request(Request::Get(keys)) => {
                    step = self.resume(Some(Reply::Values(get(&keys))));
                }
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
            let a = io.get(vec!["a".into(), "b".into()]).await;
            let b = io.get(vec!["c".into()]).await;
            (a.len(), b.get("c").cloned())
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
            Step::Done((1, Some(b"3".to_vec())))
        );
    }

    #[test]
    fn an_empty_read_never_reaches_the_host() {
        let machine = Machine::new(|io| async move { io.get(vec![]).await.len() });
        assert_eq!(machine.run(|_| panic!("asked for nothing")), 0);
    }

    #[test]
    #[should_panic(expected = "without answering")]
    fn resuming_without_a_reply_is_a_driver_bug() {
        let mut machine = Machine::new(|io| async move { io.get(vec!["a".into()]).await });
        let _ = machine.resume(None);
        let _ = machine.resume(None);
    }
}
