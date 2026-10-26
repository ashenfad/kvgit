//! The storage layout a store is stamped with, and which this code
//! reads.
//!
//! A store carries the layout version it was written in under
//! `__kvgit_version__`. This code reads one layout; a store stamped
//! with any other, older or newer, is refused before anything touches
//! it, since a newer one may hold roots this code can't see, and a
//! sweep over it would delete live data. A store with branches and no
//! stamp is from before stamps, and is refused too. A fresh store is
//! stamped when it is opened to be written.

use crate::error::Error;
use crate::json::Value;
use crate::layout::{BRANCH_HEAD_PREFIX, STORAGE_VERSION, STORAGE_VERSION_KEY};
use crate::machine::{Cas, Io, Write};
use crate::store::{dumps, safe_loads};

/// Refuse a store stamped with another layout, or one from before
/// stamps; with `stamp`, stamp a fresh one.
pub async fn check(io: &Io, stamp: bool) -> Result<(), Error> {
    loop {
        if let Some(raw) = io.get_one(STORAGE_VERSION_KEY).await? {
            return match safe_loads(&raw) {
                Some(Value::Int(v)) if v == i64::from(STORAGE_VERSION) => Ok(()),
                found => Err(Error::StorageVersion(format!(
                    "Store has kvgit storage version {}, this code supports \
                     {STORAGE_VERSION}. Use a fresh store.",
                    found.map_or_else(
                        || String::from_utf8_lossy(&raw).into_owned(),
                        |v| crate::json::dumps(&v, false)
                    )
                ))),
            };
        }
        if !io.scan(BRANCH_HEAD_PREFIX).await?.is_empty() {
            return Err(Error::StorageVersion(format!(
                "Store appears to use an older kvgit storage format. This \
                 version requires storage v{STORAGE_VERSION}. Use a fresh store."
            )));
        }
        if !stamp {
            return Ok(());
        }
        let cas = Cas {
            expect: vec![(STORAGE_VERSION_KEY.to_string(), None)],
            writes: vec![(
                STORAGE_VERSION_KEY.to_string(),
                Write::Bytes(dumps(&Value::Int(i64::from(STORAGE_VERSION)))),
            )],
            removes: vec![],
        };
        // Another opener stamping first sends this round to read its
        // stamp.
        if io.cas(cas).await? {
            return Ok(());
        }
    }
}
