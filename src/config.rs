//! Connection configuration.
use std::fmt::Display;
use std::time::Duration;

/// Connection configuration passed to [`Comms::configure`](crate::comms::Comms::configure).
///
/// Holds the broker addresses, credentials, and the delay between reconnect attempts.
/// Multiple addresses are rotated round-robin on each successive reconnect attempt.
pub struct Config {
    /// AMQP connection name (visible in the RabbitMQ management UI).
    pub name: String,
    /// Fully-qualified AMQP URIs, e.g. `amqp://user:pass@host:5672/%2f`.
    /// Built automatically by [`Config::new`].
    pub address: Vec<String>,
    /// How long to wait between reconnect attempts.
    pub reconnect_delay: Duration,
}

impl Config {
    /// Builds a [`Config`] from human-friendly parameters.
    ///
    /// Each element of `host` should be a `host:port` string.  The AMQP URI is
    /// constructed as `amqp://<user>:<password>@<host>/%2f` (default vhost).
    ///
    /// When multiple addresses are provided, each connection attempt advances to the
    /// next address in the list, wrapping around.  This happens on every attempt
    /// regardless of whether the previous one succeeded or failed.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rabbit_auto::config::Config;
    /// use std::time::Duration;
    ///
    /// // Single broker
    /// let cfg = Config::new(
    ///     "my-service".to_string(),
    ///     ["localhost:5672"].iter(),
    ///     "guest",
    ///     "guest",
    ///     Duration::from_secs(3),
    /// );
    ///
    /// // Multiple brokers — each attempt uses the next address, cycling indefinitely
    /// let cfg = Config::new(
    ///     "my-service".to_string(),
    ///     ["rabbit1:5672", "rabbit2:5672"].iter(),
    ///     "app",
    ///     "secret",
    ///     Duration::from_secs(5),
    /// );
    /// ```
    pub fn new<I: Display, T: Iterator<Item = I>>(
        name: String,
        host: T,
        user: &str,
        password: &str,
        reconnect_delay: Duration,
    ) -> Self {
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

// impl Config {
//     /// Creates a new configuration
//     /// # Arguments:
//     /// * host - the address to rabbitmq server
//     /// * user - user login name
//     /// * password - user login password
//     /// * sleep_duration - duration of the sleep before trying reconnect again to the rabbitmq server
//     pub fn new<I: Display, T: Iterator<Item = I>>(
//         name: String,
//         host: T,
//         user: &str,
//         password: &str,
//         reconnect_delay: Duration,
//     ) -> Self {
//         let mut address = Vec::new();
//         for addr in host {
//             let addr = format!("amqp://{}:{}@{}/%2f", user, password, addr);
//             address.push(addr);
//         }
//         Self {
//             name,
//             address,
//             reconnect_delay,
//         }
//     }
// }
