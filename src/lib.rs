//! Resilient, auto-reconnecting RabbitMQ consumers and publishers built on top of
//! [lapin](https://github.com/amqp-rs/lapin).
//!
//! # Core promise
//!
//! Once the initial connection succeeds, consumers and publishers will transparently
//! reconnect on any failure — the caller never sees a connection error.  If the broker
//! is unreachable at startup the library blocks until it becomes available.
//!
//! # Quick start
//!
//! ## 1. Configure the connection (once, at startup)
//!
//! ```rust,no_run
//! use rabbit_auto::config::Config;
//! use rabbit_auto::comms::Comms;
//! use std::time::Duration;
//!
//! Comms::configure(Config::new(
//!     "my-service".to_string(),
//!     ["localhost:5672"].iter(),
//!     "guest",
//!     "guest",
//!     Duration::from_secs(3),
//! )).expect("Failed to start connection manager");
//! ```
//!
//! ## 2. Declare exchanges
//!
//! ```rust,no_run
//! use rabbit_auto::comms::Comms;
//! use rabbit_auto::exchanges::create_topic_exchange;
//!
//! Comms::declare_exchange(
//!     "my-exchange".to_string(),
//!     create_topic_exchange("my-exchange".to_string(), None, None),
//! ).await;
//! ```
//!
//! ## 3. Consume messages
//!
//! ```rust,no_run
//! use rabbit_auto::stream_builder::{StreamBuilder, Deserialise};
//! use futures::StreamExt;
//! use anyhow::Result;
//!
//! struct MyMsg(String);
//! impl Deserialise for MyMsg {
//!     fn deserialise(data: Vec<u8>) -> Result<Self> {
//!         Ok(MyMsg(String::from_utf8(data)?))
//!     }
//! }
//!
//! let mut stream = StreamBuilder {
//!     exchange: "my-exchange",
//!     routing_key: "my.key",
//!     tag: "my-consumer",
//!     ..Default::default()
//! }
//! .create_auto_ack::<MyMsg>()
//! .await;
//!
//! while let Some((ack, result)) = stream.next().await {
//!     let msg = result?;
//!     println!("received: {}", msg.0);
//!     // ack is sent automatically on drop, or call ack.execute().await explicitly
//! }
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! ## 4. Publish messages
//!
//! ```rust,no_run
//! use rabbit_auto::publisher::{PublishWrapper, Serialise};
//! use futures::SinkExt;
//! use std::borrow::Cow;
//!
//! struct MyMsg(String);
//! impl Serialise for MyMsg {
//!     fn serialise(data: &Self) -> Cow<'_, [u8]> { data.0.as_bytes().into() }
//! }
//!
//! let mut publisher = PublishWrapper::<MyMsg, _>::with_static_key(
//!     "my-exchange",
//!     "my.key",
//!     /*wait_for_confirm=*/ false,
//!     None, // confirm options
//!     None, // publish options
//!     None, // message properties
//! ).await;
//!
//! publisher.send(MyMsg("hello".to_string())).await?;
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! # Feature flags
//!
//! | Flag | Effect |
//! |------|--------|
//! | `humantime` *(optional)* | Enables [`PublishProperties::with_expiration_hd`](publish_properties::PublishProperties::with_expiration_hd) for human-readable durations |
//! | `chrono` *(on by default)* | Enables [`PublishProperties::with_expiration_ch`](publish_properties::PublishProperties::with_expiration_ch) and the `create_multi_auto_ack` consumer variant |
//! | `tokio_stream` *(on by default)* | Required for `create_multi_auto_ack` |
//!
//! `multi_auto_ack` (batched ack/nack) is only compiled when **both** `chrono` and
//! `tokio_stream` are enabled.
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