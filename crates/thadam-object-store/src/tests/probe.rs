//! Probe behavior under the two store shapes: honored conditional writes
//! (InMemory) and ignored ones (LaxCreateStore, the SeaweedFS shape).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::fixture::LaxCreateStore;
use crate::probe::probe;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_that_ignores_create_conditions_still_reports_metadata() {
    // Reproduces the SeaweedFS probe bug: the conflict create overwrites
    // the probe object (attributes included) when If-None-Match is ignored,
    // so a metadata check placed after the conflict put reports false for a
    // store that does round-trip metadata — which would silently degrade
    // conflict verification to size-only.
    let store = LaxCreateStore::new();
    let caps = probe(store.store().as_ref(), "edge01")
        .await
        .expect("probe");
    assert!(
        !caps.conditional_create,
        "lax store must not claim conditional create"
    );
    assert!(
        caps.returns_metadata,
        "metadata round-trips on this store shape; the probe must see it"
    );
    assert!(caps.clock_skew.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_contextual_store_reports_all_capabilities() {
    let store = object_store::memory::InMemory::new();
    let caps = probe(&store, "edge01").await.expect("probe");
    assert!(
        caps.conditional_create,
        "InMemory enforces create-mode puts"
    );
    assert!(caps.returns_metadata);
    assert!(caps.clock_skew.is_some());
}
