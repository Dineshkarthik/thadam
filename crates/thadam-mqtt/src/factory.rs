//! `MqttSourceFactory`: how `kind: mqtt` becomes a source.

use std::fmt;
use std::sync::Arc;

use thadam_core::{BuildContext, ThadamError, Source, SourceFactory};

use crate::config::{MqttConfig, config_schema};
use crate::source::MqttSource;
use crate::stats::{MqttHandle, SessionLostHook};

/// Called with the metrics handle of every source the factory builds, so the
/// runtime's metrics layer can register gauges for it.
pub type HandleObserver = Arc<dyn Fn(&BuildContext, MqttHandle) + Send + Sync>;

/// Builds [`MqttSource`]s of kind `mqtt`.
#[derive(Clone, Default)]
pub struct MqttSourceFactory {
    session_lost: Option<SessionLostHook>,
    observer: Option<HandleObserver>,
}

impl fmt::Debug for MqttSourceFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttSourceFactory")
            .field("session_lost_hook", &self.session_lost.is_some())
            .field("observer", &self.observer.is_some())
            .finish()
    }
}

impl MqttSourceFactory {
    /// The `kind` string sources of this factory are configured with.
    pub const KIND: &'static str = "mqtt";

    /// A factory with no hooks.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every built source calls `hook` from its poll task when the broker lost
    /// a session that should have existed. The hook must not block.
    #[must_use]
    pub fn with_session_lost_hook(mut self, hook: SessionLostHook) -> Self {
        self.session_lost = Some(hook);
        self
    }

    /// Every built source reports its metrics handle to `observer`.
    #[must_use]
    pub fn with_handle_observer(mut self, observer: HandleObserver) -> Self {
        self.observer = Some(observer);
        self
    }
}

impl SourceFactory for MqttSourceFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        MqttConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        let config = MqttConfig::from_value(&cfg).map_err(|mut errors| {
            let extra = errors.len() - 1;
            let mut first = errors.swap_remove(0);
            if extra > 0 {
                first
                    .message
                    .push_str(&format!(" (and {extra} more problem(s))"));
            }
            first
        })?;
        let mut source = MqttSource::new(config, ctx);
        if let Some(hook) = &self.session_lost {
            source = source.with_session_lost_hook(hook.clone());
        }
        if let Some(observer) = &self.observer {
            observer(ctx, source.handle());
        }
        Ok(Box::new(source))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use thadam_core::{SystemClock, codes};
    use serde_json::json;

    use super::*;

    fn ctx() -> BuildContext {
        BuildContext::new(
            "mqtt-1",
            Arc::new(SystemClock::new()),
            std::path::PathBuf::new(),
        )
    }

    fn good() -> serde_json::Value {
        json!({ "url": "mqtt://localhost", "client_id": "c", "topics": [{ "filter": "a/#" }] })
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = MqttSourceFactory::new();
        assert_eq!(factory.kind(), "mqtt");
        assert_eq!(factory.config_schema()["title"], "MQTT source");
        assert!(factory.validate(&good()).is_ok());
        let errors = factory.validate(&json!({})).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_MISSING);
    }

    #[test]
    fn build_returns_a_source_with_the_configured_ack_mode() {
        let source = MqttSourceFactory::new().build(good(), &ctx()).unwrap();
        assert_eq!(
            source.capabilities().ack_mode,
            thadam_core::AckMode::OnCommit
        );

        let mut cfg = good();
        cfg["ack_mode"] = json!("on_receive");
        let source = MqttSourceFactory::new().build(cfg, &ctx()).unwrap();
        assert_eq!(
            source.capabilities().ack_mode,
            thadam_core::AckMode::OnReceive
        );
    }

    #[test]
    fn build_reports_the_first_problem_and_counts_the_rest() {
        let mut cfg = good();
        cfg["client_id"] = json!("");
        cfg["topics"] = json!([]);
        let err = MqttSourceFactory::new().build(cfg, &ctx()).err().unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("and 1 more"), "{}", err.message);
    }

    #[test]
    fn the_observer_receives_a_handle_for_every_build() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let factory =
            MqttSourceFactory::new().with_handle_observer(Arc::new(move |ctx, handle| {
                sink.lock()
                    .unwrap()
                    .push((ctx.component_id.to_string(), handle.stats().connected));
            }));
        factory.build(good(), &ctx()).unwrap();
        assert_eq!(*seen.lock().unwrap(), [("mqtt-1".to_owned(), false)]);
    }
}
