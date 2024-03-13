//!
//! Wrappers around RabbitMQ connection to indefinitely (after the first successful connection) consume,
//! or publish without any errors. If the connection disconnects, the consumer and publisher will wait
//! until the connection is established back again.
//!
// macro_rules! log_error {
//     ($action: expr) => {
//         if let Err(err) = $action {
//             log::error!("{}: {}", stringify!($action), err);
//         }
//     };
//     ($action: expr, $msg: expr) => {
//         if let Err(err) = $action {
//             log::error!("{}: {}", $msg, err);
//         }
//     };
// }

#[cfg(all(feature = "tokio_runtime", feature = "async_std_runtime"))]
compile_error!("Tokio and Async-std are mutually exclusive");

pub mod config;
pub mod comms;
pub mod consumer;

pub mod exchanges;

pub mod publisher;

pub mod auto_ack;

pub mod stream_builder;


pub mod publish_properties;