//! Configuration for the rabbitmq connection.
//!
//!
use std::fmt::Display;
use std::time::Duration;

/// Configuration for the rabbitmq connection
pub struct Config {
    pub name: String,
    pub address: Vec<String>,
    pub reconnect_delay: Duration,
}

impl Config {
    /// Creates a new configuration
    /// # Arguments:
    /// * host - the address to rabbitmq server
    /// * user - user login name
    /// * password - user login password
    /// * sleep_duration - duration of the sleep before trying reconnect again to the rabbitmq server
    pub fn new<I: Display, T: Iterator<Item = I>>(name: String, host: T, user: &str, password: &str, reconnect_delay: Duration) -> Self {
        let mut address = Vec::new();
        for addr in host {
            let addr = format!("amqp://{}:{}@{}/%2f", user, password, addr);
            address.push(addr);
        }
        Self {
            name,
            address,
            reconnect_delay,
        }
    }
}