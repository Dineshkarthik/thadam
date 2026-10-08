//! Connector contracts: sources, sinks, services, factories, and the
//! acknowledgement machinery between them and the runtime.

mod ack;
mod context;
mod factory;
mod sink;
mod source;

pub use ack::{AckHandle, AckOutcome, AckStatus, AckTarget, AckToken, ConnectionEpoch, EpochGate};
pub use context::{BuildContext, ServiceContext, SharedObjects, SinkContext, SourceContext};
pub use factory::{BatchSinkFactory, SegmentSinkFactory, Service, ServiceFactory, SourceFactory};
pub use sink::{BatchSink, DeliveryClass, SegmentSink, SinkBatch, SinkCapabilities, WriteOutcome};
pub use source::{AckMode, BatchLimits, Source, SourceBatch, SourceCapabilities};
