//! Configuration for the rabbitmq connection.
//!
//!
use std::time::Duration;

/// Configuration for the rabbitmq connection
pub struct Config {
    pub address: String,
    pub sleep_duration: Duration,
}

impl Config {
    /// Creates a new configuration
    /// # Arguments:
    /// * host - the address to rabbitmq server
    /// * user - user login name
    /// * password - user login password
    /// * sleep_duration - duration of the sleep before trying reconnect again to the rabbitmq server
    pub fn new(host: &str, user: &str, password: &str, sleep_duration: Duration) -> Self {
        let address = format!("amqp://{}:{}@{}/%2f", user, password, host);
        Self {
            address,
            sleep_duration,
        }
    }
}