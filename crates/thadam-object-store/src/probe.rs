//! The startup capability probe (plan 5.8): does the store enforce
//! `If-None-Match`, does it return user metadata, and how far is its clock
//! from ours.
//!
//! The probe writes one small object under `.thadam-shipper-probe/` and
//! deletes it best-effort (a least-privilege bucket policy may not grant
//! `DeleteObject`; a documented lifecycle rule cleans up). Probe errors are
//! returned to the caller, which retries them with the outer backoff: a
//! device that boots before its uplink is up is the common case.

use std::time::{Duration, SystemTime};

use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions};

use crate::error::ShipError;
use crate::keys::{SHA256_ATTR, sha256_attr, store_path};

/// What the startup probe found out about the store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StoreCapabilities {
    /// The store enforces `If-None-Match: *` (a second create failed). When
    /// false, `conditional_put: auto` falls back to overwrite.
    pub conditional_create: bool,
    /// |server clock - local clock| estimated from the probe object's
    /// `last_modified`, when it could be measured.
    pub clock_skew: Option<Duration>,
    /// HEAD/GET-of-a-range returns user metadata, so a conflict can be
    /// verified by `thadam-sha256` instead of size alone.
    pub returns_metadata: bool,
}

fn probe_name(node_id: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(".thadam-shipper-probe/{node_id}-{nanos:x}")
}

/// Runs the probe against `store` on behalf of `node_id`.
pub(crate) async fn probe(
    store: &dyn ObjectStore,
    node_id: &str,
) -> Result<StoreCapabilities, ShipError> {
    let key = probe_name(node_id);
    let path = store_path(&key);

    let mut attrs = object_store::Attributes::new();
    attrs.insert(sha256_attr(), "0".repeat(64).into());
    let opts = PutOptions {
        mode: PutMode::Create,
        attributes: attrs,
        ..PutOptions::default()
    };

    let before = SystemTime::now();
    store.put_opts(&path, "probe".into(), opts).await?;
    let after = SystemTime::now();

    let meta = store.head(&path).await?;
    let object_nanos = meta.last_modified.timestamp_nanos_opt().unwrap_or(0);
    let local_mid_nanos = before
        .checked_add(after.duration_since(before).unwrap_or(Duration::ZERO) / 2)
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as i128)
        .unwrap_or(0);
    let skew_nanos = (i128::from(object_nanos) - local_mid_nanos).unsigned_abs();
    let skew = Duration::from_nanos(u64::try_from(skew_nanos).unwrap_or(u64::MAX));

    let conflict = store
        .put_opts(&path, "probe-conflict".into(), PutMode::Create.into())
        .await;
    let conditional_create = matches!(conflict, Err(object_store::Error::AlreadyExists { .. }));

    let get_opts = GetOptions {
        range: Some(GetRange::Bounded(0..1)),
        ..Default::default()
    };
    let returns_metadata = match store.get_opts(&path, get_opts).await {
        Ok(result) => result.attributes.iter().any(
            |(a, _)| matches!(a, object_store::Attribute::Metadata(m) if m.as_ref() == SHA256_ATTR),
        ),
        Err(_) => false,
    };

    if let Err(e) = store.delete(&path).await {
        tracing::warn!(error = %e,
            "probe object could not be deleted (least-privilege policy?); a lifecycle rule should clean .thadam-shipper-probe/");
    }

    Ok(StoreCapabilities {
        conditional_create,
        clock_skew: Some(skew),
        returns_metadata,
    })
}
