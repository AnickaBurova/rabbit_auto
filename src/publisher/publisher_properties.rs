//! Publisher message-property helpers.
//!
//! An alternative to [`crate::publish_properties::PublishProperties`] that
//! exposes a smaller, typed API covering the most-used AMQP properties.

use std::time::{Duration, SystemTime};
use lapin::protocol::basic::AMQPProperties;
use crate::publisher::GetBP;

/// Typed builder for the most commonly used AMQP message properties.
///
/// Convert to a [`GetBP`] callback (via `Into<Option<GetBP>>`) and pass it to
/// [`PublishWrapper::with_static_key`](crate::publisher::PublishWrapper::with_static_key).
///
/// `Into<Option<GetBP>>` returns `None` when all properties are at their
/// defaults, which tells the publisher to use lapin's default properties.
///
/// For properties not covered here (e.g. `content_type`, `correlation_id`) use
/// [`crate::publish_properties::PublishProperties`] directly.
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::publisher::publisher_properties::{Properties, DeliveryMode};
/// use rabbit_auto::publisher::PublishWrapper;
/// use std::time::Duration;
///
/// # async fn example() {
/// let props: Option<rabbit_auto::publisher::GetBP> = Properties::expiration(Duration::from_secs(60))
///     .with_delivery_mode(DeliveryMode::Persistent)
///     .into();
///
/// let publisher = PublishWrapper::<Vec<u8>, _>::with_static_key(
///     "my-exchange",
///     "my.key",
///     false,
///     None,
///     None,
///     props,
/// ).await;
/// # }
/// ```
#[derive(Default)]
pub struct Properties {
    expiration_value: Option<Duration>,
    delivery_mode_value: Option<DeliveryMode>,
    timestamp_value: Option<SystemTime>,
}

/// AMQP delivery (persistence) mode.
///
/// See <https://www.rabbitmq.com/publishers.html#message-properties>.
pub enum DeliveryMode {
    /// Message is stored in memory only; lost on broker restart.
    Transient,
    /// Message is written to disk; survives broker restart (requires a durable queue).
    Persistent,
}

impl Into<AMQPProperties> for Properties {
    fn into(self) -> AMQPProperties {
        let mut properties = AMQPProperties::default();
        if let Some(expiration) = self.expiration_value {
            properties = properties.with_expiration(expiration.as_millis().to_string().into());
        }
        if let Some(delivery_mode) = self.delivery_mode_value {
            properties = properties.with_delivery_mode(match delivery_mode {
                DeliveryMode::Transient => {1}
                DeliveryMode::Persistent => {2}
            });
        }
        if let Some(timestamp) = self.timestamp_value {
            if let Ok(timestamp) = timestamp.duration_since(std::time::UNIX_EPOCH) {
                let timestamp = timestamp.as_millis();
                properties = properties.with_timestamp(timestamp as u64);
            }
        }
        properties
    }
}

impl Properties {
    /// Creates a `Properties` with only the expiration set.
    pub fn expiration(expiration: Duration) -> Self {
        Self {
            expiration_value: Some(expiration),
            ..Default::default()
        }
    }

    /// Sets (or replaces) the message expiration TTL.
    pub fn with_expiration(mut self, expiration: Duration) -> Self {
        self.expiration_value = Some(expiration);
        self
    }

    /// Sets (or replaces) the delivery mode.
    pub fn with_delivery_mode(mut self, mode: DeliveryMode) -> Self {
        self.delivery_mode_value = Some(mode);
        self
    }

    /// Returns `true` if at least one property differs from the default.
    pub fn no_default(&self) -> bool {
        self.expiration_value.is_none() || self.timestamp_value.is_none() || self.timestamp_value.is_none()
    }
}

impl Into<Option<GetBP>> for Properties {
    fn into(self) -> Option<GetBP> {
        if self.no_default() {
            let properties: AMQPProperties = self.into();
            Some(Box::pin(move|| properties.clone()))
        } else {
            None
        }
    }
}