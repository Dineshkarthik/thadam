//! Cooperative cancellation.

use tokio_util::sync::CancellationToken;

/// The shutdown signal components poll or await.
///
/// Cancellation is cooperative: the runtime never drops a connector future
/// mid-operation (except [`Source::next_batch`](crate::Source::next_batch),
/// which is cancel-safe by contract). Clones share one signal; a
/// [`child`](Shutdown::child) is triggered by its parent but can also be
/// triggered on its own.
#[derive(Clone, Debug, Default)]
pub struct Shutdown(CancellationToken);

impl Shutdown {
    /// A new, untriggered signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// A signal that triggers when this one does, or on its own.
    pub fn child(&self) -> Shutdown {
        Shutdown(self.0.child_token())
    }

    /// Triggers shutdown for this signal and its children.
    pub fn trigger(&self) {
        self.0.cancel();
    }

    /// Whether shutdown has been triggered.
    pub fn is_triggered(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Completes once shutdown is triggered. Cancel-safe.
    pub async fn triggered(&self) {
        self.0.cancelled().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trigger_wakes_waiters_on_clones() {
        let shutdown = Shutdown::new();
        let waiter = tokio::spawn({
            let s = shutdown.clone();
            async move { s.triggered().await }
        });
        assert!(!shutdown.is_triggered());
        shutdown.trigger();
        waiter.await.unwrap();
        assert!(shutdown.is_triggered());
    }

    #[tokio::test]
    async fn child_follows_parent_but_not_the_reverse() {
        let parent = Shutdown::new();
        let child = parent.child();
        child.trigger();
        assert!(child.is_triggered());
        assert!(!parent.is_triggered());

        let child = parent.child();
        parent.trigger();
        assert!(child.is_triggered());
        child.triggered().await;
    }
}
