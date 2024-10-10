//! Wrapper helper functions for publisher properties

use std::time::{Duration, SystemTime};
use lapin::protocol::basic::AMQPProperties;
use crate::publisher::GetBP;


/// Wrapper for AMQPProperties
/// https://www.rabbitmq.com/publishers.html#message-properties
#[derive(Default)]
pub struct Properties {
    expiration_value: Option<Duration>,
    delivery_mode_value: Option<DeliveryMode>,
    timestamp_value: Option<SystemTime>,
}

/// See https://www.rabbitmq.com/persistence-conf.html
pub enum DeliveryMode {
    Transient,
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
    /// Creates properties with expiration
    pub fn expiration(expiration: Duration) -> Self {
        Self {
            expiration_value: Some(expiration),
            ..Default::default()
        }
    }

    /// Sets the expiration in the properties
    pub fn with_expiration(mut self, expiration: Duration) -> Self {
        self.expiration_value = Some(expiration);
        self
    }

    /// Test if any properties are set
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