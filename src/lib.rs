//!
//! Wrappers around RabbitMQ connection to indefinitely (after the first successful connection) consume,
//! or publish without any errors. If the connection disconnects, the consumer and publisher will wait
//! until the connection is established back again.
//!
// macro_rules! log_error {
//     ($action: expr) => {
//         if let Err(err) = $action {
//             tracing::error!("{}: {}", stringify!($action), err);
//         }
//     };
//     ($action: expr, $msg: expr) => {
//         if let Err(err) = $action {
//             tracing::error!("{}: {}", $msg, err);
//         }
//     };
// }

pub mod comms;
pub mod config;
pub mod consumer;

pub mod exchanges;

pub mod publisher;

pub mod auto_ack;

pub mod stream_builder;

pub mod publish_properties;

#[cfg(all(feature = "tokio_stream", feature = "chrono"))]
pub mod multi_auto_ack;