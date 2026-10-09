//! HTTP source connector.
//!
//! Two modes, selected by `mode` in the connector config:
//!
//! - `listen`: an HTTP server accepts `POST {path}` and emits each body as one
//!   message. The response is sent only after the message is durably committed.
//! - `poll`: periodically fetches one or more URLs and emits each successful
//!   response body as one message. Delivery is at-most-once per response.

mod config;
#[cfg(test)]
mod conformance;
mod error;
mod factory;
mod listen;
mod poll;
mod queue;
mod source;
mod tls;

pub use config::{
    DurationSpec, HttpConfig, HttpMode, ListenConfig, PollConfig, PollMethod, TlsConfig,
    config_schema,
};
pub use error::HttpError;
pub use factory::HttpSourceFactory;
pub use source::HttpSource;
