//! The GC lease every sweep runs under, and writing around it.
//!
//! The lease is `{"owner": <id>, "expires": <unix time>}` under
//! `__gc_lease__`. A record whose expiry has passed, or that doesn't
//! decode, is no lease, so a sweep that dies holding one blocks the
//! store only until it expires. A sweep takes the lease by writing a
//! record of its own, and every write a sweep must not interleave with
//! expects the record its writer read after waiting any live lease out.
//! So no such write lands while a sweep runs, and one that lands before
//! carries what protects it: a commit's batch, its in-flight marker.

use crate::json::Value;
use crate::layout::{GC_LEASE_KEY, STORAGE_VERSION_KEY};
use crate::machine::{Cas, HostFailed, Io, Write};
use crate::store::{dumps, safe_loads};

/// Seconds an in-flight marker protects an unpublished commit.
pub const IN_FLIGHT_TTL: f64 = 600.0;

/// Seconds a writer sleeps between checks while a lease is live: the
/// store has no wait primitive, so waiting is polling.
pub const GC_WAIT_POLL: f64 = 0.05;

/// When the lease in `raw` runs out; 0 for no lease. Bytes that aren't
/// a record with a numeric expiry count as expired, so damage under the
/// key never wedges the store.
pub fn lease_expiry(raw: Option<&[u8]>) -> f64 {
    let Some(record) = raw.and_then(safe_loads) else {
        return 0.0;
    };
    match record.get("expires") {
        Some(Value::Int(n)) => *n as f64,
        Some(Value::BigInt(digits)) => digits.parse().unwrap_or(0.0),
        Some(Value::Float(x)) => *x,
        _ => 0.0,
    }
}

/// Wait until no live lease is held, and return the lease record then:
/// what a write that must not land under a sweep expects the key to
/// hold. `raw` is the record if the caller has just read it; otherwise
/// this reads it. While a lease is live this polls, sleeping at most
/// until that lease's own expiry.
pub async fn wait_for_gc(
    io: &Io,
    raw: Option<Option<Vec<u8>>>,
) -> Result<Option<Vec<u8>>, HostFailed> {
    let mut raw = match raw {
        Some(raw) => raw,
        None => io.get_one(GC_LEASE_KEY).await?,
    };
    while raw.is_some() {
        let remaining = lease_expiry(raw.as_deref()) - io.wall();
        if remaining <= 0.0 {
            return Ok(raw);
        }
        io.wait(remaining.min(GC_WAIT_POLL)).await?;
        raw = io.get_one(GC_LEASE_KEY).await?;
    }
    Ok(None)
}

/// A batch expecting `lease` beside its own expectations.
pub fn under_lease(lease: Option<Vec<u8>>, mut cas: Cas) -> Cas {
    cas.expect.insert(0, (GC_LEASE_KEY.to_string(), lease));
    cas
}

/// Land a batch unless a sweep began since `lease` was read: `Some(true)`
/// when it landed, `Some(false)` when one of its own expectations
/// failed, and `None` when the lease moved, so whatever the caller
/// checked before writing may no longer hold.
pub async fn try_land(
    io: &Io,
    lease: Option<Vec<u8>>,
    cas: Cas,
) -> Result<Option<bool>, HostFailed> {
    if io.cas(under_lease(lease.clone(), cas)).await? {
        return Ok(Some(true));
    }
    if io.get_one(GC_LEASE_KEY).await? != lease {
        return Ok(None);
    }
    Ok(Some(false))
}

/// Raise the store's version stamp to `version` if it is lower. Each
/// write expects the bytes it decided from, so two writers stamping
/// different versions can't leave the lower one last; a stamp only
/// rises, so this ends.
pub async fn stamp_version_at_least(io: &Io, version: i64) -> Result<(), HostFailed> {
    loop {
        let raw = io.get_one(STORAGE_VERSION_KEY).await?;
        match raw.as_deref().and_then(safe_loads) {
            Some(Value::Int(current)) if current >= version => return Ok(()),
            Some(Value::BigInt(digits)) if !digits.starts_with('-') => return Ok(()),
            _ => {}
        }
        let cas = Cas {
            expect: vec![(STORAGE_VERSION_KEY.to_string(), raw)],
            writes: vec![(
                STORAGE_VERSION_KEY.to_string(),
                Write::Bytes(dumps(&Value::Int(version))),
            )],
            removes: vec![],
        };
        if io.cas(cas).await? {
            return Ok(());
        }
    }
}
