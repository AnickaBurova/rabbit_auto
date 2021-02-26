//! Helper to create a connection and keep it connected.


use lapin::{Connection, Channel, ConnectionProperties};
use std::sync::Arc;
use futures::lock::Mutex;
use std::pin::Pin;
use futures::Future;
use anyhow::{Result, Context};
use crate::config::Config;

/// Result of the creator function
pub type CreatorResult<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;
/// Creator function
pub type Creator<T> = Pin<Box<dyn Fn(Channel) -> CreatorResult<T> + Send + Sync>>;

enum State {
    /// Configured, but not connected at least once
    Configured {
        config: Config,
    },
    /// Connected and running
    Connected {
        config: Config,
        connection: Connection,
    },
}

/// RabbitMQ connection singleton
pub struct Comms {
    state: Option<State>,
}


impl Comms {
    /// Creates an uninitialised connection
    fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self { state: None }))
    }

    fn get_connection(&mut self) -> &mut Connection {
        match &mut self.state {
            Some(State::Connected { connection, .. }) => connection,
            _ => unreachable!(),
        }
    }


    /// Connect to the rabbitmq server.
    /// The singleton must be configured before the first use.
    /// This needs to establish at least once a good connection (to ensure a correct configuration),
    /// before it goes to the default behaviour.
    /// The default behaviour is, it will only finish if the connection is establish, otherwise this
    /// will not finish and will wait for connection.
    async fn connect(&mut self) -> Result<&mut Connection> {
        let (config, repeat) = match self.state.take() {
            None => return Err(anyhow::anyhow!("RabbitMQ connection is not configured")),
            Some(State::Connected { connection, config }) => {
                log::trace!("Testing validity of rabbit connection");
                let status = connection.status();
                if status.connected() {
                    // if the connection is valid, then just return
                    log::trace!("Rabbit is ok");
                    self.state = Some(State::Connected { connection, config });
                    return Ok(self.get_connection());
                }
                (config, true)
            }
            // if this is the very first attempt to make a connection, it is not allowed to repeat,
            // because the failure might be just misconfigured rabbit instead of temporary not available
            Some(State::Configured { config }) => (config, false),
        };
        loop {
            log::trace!("Connecting to rabbitmq");
            let connection = Connection::connect(
                &config.address,
                ConnectionProperties::default().with_default_executor(8),
            )
                .await;
            match connection {
                Ok(connection) => {
                    log::trace!("Connected");
                    self.state = Some(State::Connected { connection, config });
                    return Ok(self.get_connection());
                }
                Err(err) => {
                    if repeat {
                        log::error!("Failed to connect: {}", err);
                        // wait a second, and try to connect again
                        #[cfg(feature = "tokio_runtime")]
                        tokio::time::sleep(config.sleep_duration).await;
                    } else {
                        return Err(err).context("Failed to connect not even once, check rabbitmq configuration");
                    }
                }
            }
        }
    }

    /// Creates a channel. This will only return the channel if the connection is valid, otherwise will
    /// wait to get the connection, or fail for misconfigured connection.
    pub async fn create_channel() -> Result<Channel> {
        // if there is another attempt trying to get to connect, it will wait here until this one is finished.
        let this = Comms::get();
        let mut this = this.lock().await;
        loop {
            let connection = this.connect().await?;
            log::trace!("Creating a channel");
            match connection.create_channel().await {
                Ok(channel) => {
                    log::trace!("Channel {} created", channel.id());
                    return Ok(channel);
                },
                Err(err) => {
                    log::error!("Failed to create a channel: {}", err);
                    continue;
                }
            };
        }
    }

    /// Creates and object on a freshly created channel.
    /// ***creator*** arguments is called with the channel, and the ***creator*** handles the
    /// creation of the object.
    /// The channel has to be created here, otherwise this function wouldn't be able to confirm
    /// a valid connection. This is trying to establish a correct connection and in case of loosing the
    /// connection, it will repeat until success (if the channel is provided, and the connection
    /// is interrupted, that channel is no longer valid).
    /// # Arguments:
    /// * creator - a function which creates the object we want. The input here is the valid channel.
    ///             If the channel is not valid anymore during the creation, just return Err and
    ///             this function will recreate everything again.
    pub async fn create_channel_and_object<T: Send>(creator: Creator<T>) -> Result<(Creator<T>, T)> {
        let this = Comms::get();
        let mut this = this.lock().await;
        loop {
            let connection = this.connect().await?;
            log::trace!("Creating a channel and the object");
            let channel =
                match connection.create_channel().await {
                    Ok(channel) => {
                        log::trace!("Channel {} created", channel.id());
                        channel
                    },
                    Err(err) => {
                        log::error!("Failed to create a channel: {}", err);
                        continue;
                    }
                };
            log::trace!("Running the creator");
            match creator(channel).await {
                Ok(obj) => {
                    log::trace!("The object has been created");
                    return Ok((creator, obj))
                },
                Err(err) => {
                    log::error!("Failed to create an object: {}", err);
                }
            }
        }
    }

    /// The connection needs to be configured before the first using
    pub async fn configure(config: Config) {
        let this = Self::get();
        let mut this = this.lock().await;
        this.state = Some(State::Configured { config });
    }

    /// Gets the Comms's Singleton
    pub fn get() -> Arc<Mutex<Self>> {
        lazy_static::lazy_static! {
            static ref SINGLETON: Arc<Mutex<Comms>> = Comms::new();
        }

        SINGLETON.clone()
    }
}
