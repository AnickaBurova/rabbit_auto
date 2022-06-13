//! Helper to create a connection and keep it connected.


use lapin::{Connection, Channel, ConnectionProperties};
use std::sync::Arc;
use futures::lock::Mutex;
use std::pin::Pin;
use futures::Future;
use lapin::types::{AMQPValue, ShortUInt};
use crate::comms::comms_data::Data;
use crate::config::Config;

/// Result of the creator function
pub type CreatorResult<T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send>>;
/// Creator function
pub type Creator<T> = Pin<Box<dyn Fn(Arc<Mutex<Channel>>) -> CreatorResult<T> + Send + Sync>>;


mod comms_data;

/// RabbitMQ connection singleton
pub struct Comms {
    /// Login and config data
    config: Option<Config>,
    /// Counter
    connection_attempt: usize,
    /// The connection
    connection: Option<Data>,
}


impl Comms {
    /// Creates an uninitialised connection
    fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self { config: None, connection_attempt: 0, connection : None }))
    }



    /// Connect to the rabbitmq server.
    /// The singleton must be configured before the first use.
    /// This needs to establish at least once a good connection (to ensure a correct configuration),
    /// before it goes to the default behaviour.
    /// The default behaviour is, it will only finish if the connection is establish, otherwise this
    /// will not finish and will wait for connection.
    async fn connect() {
        let this = Self::get();
        let mut this = this.lock().await;
        if let Some(con) = this.connection.as_ref() {
            log::trace!("Testing validity of the rabbit connection");
            let status = con.get_status();
            if status.connected() {
                // if the connection is valid, then just return
                log::trace!("Rabbit is ok");
                return;
            }
        } else {
            log::trace!("Connection is invalid, trying to connect");
        }
        let config = if let Some(config) = this.config.as_ref() {
            config
        } else {
            log::error!("Auto rabbit was not initialised calling `Comms::configure`");
            std::process::exit(-1);
        };
        loop {
            log::trace!("Connecting to the rabbitmq");
            let mut properties =
                ConnectionProperties::default().with_connection_name(config.name.clone().into());
            properties
                .client_properties
                .insert("channel_max".into(), AMQPValue::ShortUInt(comms_data::MAX_CHANNELS as ShortUInt));

            let addr = &config.address[this.connection_attempt % config.address.len()];
            let con = Connection::connect(addr, properties).await;
            match con {
                Ok(connection) => {
                    log::trace!("Connected");
                    this.connection = Some(Data::new(connection));
                    return;
                }
                Err(err) => {
                    log::error!("Failed to connect: {}", err);
                    // wait a second, and try to connect again
                    #[cfg(feature = "tokio_runtime")]
                    tokio::time::sleep(config.reconnect_delay).await;
                    #[cfg(feature = "async_std_runtime")]
                    async_std::task::sleep(config.reconnect_delay).await;
                }
            }
        }




    }

    /// Creates a channel. This will only return the channel if the connection is valid, otherwise will
    /// wait to get the connection, or fail for misconfigured connection.
    pub async fn create_channel() -> Arc<Mutex<Channel>> {
        loop {
            Self::connect().await;
            let this = Comms::get();
            let mut this = this.lock().await;
            if let Some(data) = this.connection.as_mut() {
                match data.create_channel().await {
                    Ok(channel) => return channel,
                    Err(err) => {
                        log::error!("Failed to create a channel: {}", err);
                        log_error!(data.close().await);
                        continue;
                    }
                };
            }
        }
    }

    /// Creates and object on a freshly created channel.
    /// ***creator*** arguments is called with the channel, and the ***creator*** handles the
    /// creation of the object.
    pub async fn create_object<T: Send>(creator: Creator<T>) -> (Creator<T>, T) {
        loop {
            Self::connect().await;
            let this = Comms::get();
            let mut this = this.lock().await;
            let channel = if let Some(data) = this.connection.as_mut() {
                log::trace!("Creating a channel on the rabbit");
                match data.create_channel().await {
                    Ok(channel) => channel,
                    Err(err) => {
                        log::error!("Failed to create a channel: {}", err);
                        log_error!(data.close().await);
                        continue;
                    }
                }
            } else {
                continue;
            };
            log::trace!("Running creator on rabbit");
            match creator(channel).await {
                Ok(obj) => return (creator, obj),
                Err(err) => {
                    log::error!("Failed to create an object: {}", err);
                    let this = Comms::get();
                    let mut this = this.lock().await;
                    if let Some(data) = this.connection.as_mut() {
                        log_error!(data.close().await);
                    }
                }
            }
        }
    }
    // /// Creates and object on a freshly created channel.
    // /// ***creator*** arguments is called with the channel, and the ***creator*** handles the
    // /// creation of the object.
    // /// The channel has to be created here, otherwise this function wouldn't be able to confirm
    // /// a valid connection. This is trying to establish a correct connection and in case of loosing the
    // /// connection, it will repeat until success (if the channel is provided, and the connection
    // /// is interrupted, that channel is no longer valid).
    // /// # Arguments:
    // /// * creator - a function which creates the object we want. The input here is the valid channel.
    // ///             If the channel is not valid anymore during the creation, just return Err and
    // ///             this function will recreate everything again.
    // pub async fn create_channel_and_object<T: Send>(creator: Creator<T>) -> Result<(Creator<T>, T)> {
    //     let this = Comms::get();
    //     let mut this = this.lock().await;
    //     loop {
    //         let connection = this.connect().await?;
    //         log::trace!("Creating a channel and the object");
    //         let channel =
    //             match connection.create_channel().await {
    //                 Ok(channel) => {
    //                     log::trace!("Channel {} created", channel.id());
    //                     channel
    //                 },
    //                 Err(err) => {
    //                     log::error!("Failed to create a channel: {}", err);
    //                     continue;
    //                 }
    //             };
    //         log::trace!("Running the creator");
    //         match creator(channel).await {
    //             Ok(obj) => {
    //                 log::trace!("The object has been created");
    //                 return Ok((creator, obj))
    //             },
    //             Err(err) => {
    //                 log::error!("Failed to create an object: {}", err);
    //             }
    //         }
    //     }
    // }

    /// The connection needs to be configured before the first using
    pub async fn configure(config: Config) {
        let this = Self::get();
        let mut this = this.lock().await;
        this.config = Some(config);
    }

    /// Gets the Comms's Singleton
    pub fn get() -> Arc<Mutex<Self>> {
        lazy_static::lazy_static! {
            static ref SINGLETON: Arc<Mutex<Comms>> = Comms::new();
        }

        SINGLETON.clone()
    }
}
