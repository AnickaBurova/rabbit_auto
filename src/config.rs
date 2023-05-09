//! Configuration for the rabbitmq connection.
//!
//!
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use executor_trait::FullExecutor;
use reactor_trait::Reactor;

/// Configuration for the rabbitmq connection
pub struct Config<E, R>
where
    E: FullExecutor,
    R: Reactor,
{
    pub name: String,
    pub address: Vec<String>,
    pub reconnect_delay: Duration,
    pub executor: Arc<E>,
    pub reactor: Arc<R>,
}

impl<E, R> Config<E, R>
    where
        E: FullExecutor,
        R: Reactor,
{
    /// Creates a new configuration
    /// # Arguments:
    /// * host - the address to rabbitmq server
    /// * user - user login name
    /// * password - user login password
    /// * sleep_duration - duration of the sleep before trying reconnect again to the rabbitmq server
    pub fn new<I: Display, T: Iterator<Item = I>>(
        name: String,
        host: T,
        user: &str,
        password: &str,
        reconnect_delay: Duration,
        executor: E,
        reactor: R,
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
            executor: Arc::new(executor),
            reactor: Arc::new(reactor),
        }
    }
}