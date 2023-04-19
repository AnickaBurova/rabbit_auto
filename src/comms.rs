//! Helper to create a connection and keep it connected.


use std::collections::HashMap;
use lapin::{Connection, Channel, ConnectionProperties};
use std::sync::{Arc};
use futures::lock::Mutex;
use std::pin::Pin;
use futures::Future;
use lapin::types::{AMQPValue, ShortUInt};
use crate::comms::comms_data::Data;
use crate::config::Config;
#[cfg(feature = "tokio_runtime")]
use tokio::sync::RwLock;
#[cfg(feature = "async_std_runtime")]
use async_std::sync::RwLock;
use crate::exchanges::DeclareExchange;

/// Result of the creator function
pub type CreatorResult<T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send>>;
/// Creator function
pub type Creator<T> = Pin<Box<dyn Fn(Arc<Channel>, Arc<RwLock<HashMap<String, DeclareExchange>>>) -> CreatorResult<T> + Send + Sync>>;



mod comms_data;

/// RabbitMQ connection singleton
pub struct Comms {
    /// Login and config data
    config: Option<Config>,
    /// Counter
    connection_attempt: usize,
    exchanges: Arc<RwLock<HashMap<String, DeclareExchange>>>,
    /// The connection
    connection: Option<Data>,
    // should_sleep: bool,
}


impl Comms {
    /// Creates an uninitialised connection
    fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(
            Self {
                config: None,
                connection_attempt: 0,
                connection : None,
                exchanges: Arc::new(RwLock::new(HashMap::new())),
                // should_sleep: false,
            }))
    }

    /// Connect to the rabbitmq server.
    /// The singleton must be configured before the first use.
    /// This needs to establish at least once a good connection (to ensure a correct configuration),
    /// before it goes to the default behaviour.
    /// The default behaviour is, it will only finish if the connection is establish, otherwise this
    /// will not finish and will wait for connection.
    async fn connect() {
        log::info!("Connecting to rabbitmq server");
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
        // stop the current connection if any
        if let Some(con) = this.connection.as_mut() {
            if let Err(err) = con.close().await {
                log::error!("Failed to close connection: {}", err);
            }
        }
        let config = if let Some(config) = this.config.as_ref() {
            config
        } else {
            log::error!("Auto rabbit was not initialised calling `Comms::configure`");
            std::process::exit(-1);
        };

        let executor = config.executor.clone();
        let reactor = config.reactor.clone();

        // if this.should_sleep {
        //     this.should_sleep = false;
        // }

        loop {
            log::trace!("Connecting to the rabbitmq");

            let mut properties =
                ConnectionProperties::default().with_connection_name(config.name.clone().into());
            properties.executor = Some(executor.clone());
            properties.reactor = Some(reactor.clone());
            properties
                .client_properties
                .insert("channel_max".into(), AMQPValue::ShortUInt(comms_data::MAX_CHANNELS as ShortUInt));

            let addr = config.address[this.connection_attempt % config.address.len()].clone();
            let con = Connection::connect(&addr, properties).await;
            // let ( con, promise) = PinkySwear::<lapin::Result<Connection>>::new();
            // executor.spawn(Box::pin(async move{
            //     let addr = addr.as_ref();
            //     let connector = Connection::connect(addr, properties);
            //     let con = connector.await;
            //     promise.swear(con);
            // }));

            match con {
                Ok(connection) => {
                    log::trace!("Connected");
                    this.connection = Some(Data::new(connection));
                    return;
                }
                Err(err) => {
                    log::error!("Failed to connect: {}", err);
                    log::trace!("Going to sleep for {:?}", config.reconnect_delay);
                    reactor.sleep(config.reconnect_delay).await;
                    // this.should_sleep = true;
                    // wait a second, and try to connect again
                    // #[cfg(feature = "tokio_runtime")]
                    // {
                    //     // let executor = tokio_executor_trait::Tokio::current();
                    //     // executor.spawn(Box::pin(async move {
                    //     //     let mut counter = 0;
                    //     //     while counter < 10 {
                    //     //         log::info!("Spawned debug counter: {}", counter);
                    //     //         tokio_reactor_trait::Tokio.sleep(std::time::Duration::from_secs(1)).await;
                    //     //         counter += 1;
                    //     //     }
                    //     // }));
                    //     log::trace!("Using tokio runtime");
                    //
                    //     tokio_reactor_trait::Tokio.sleep(config.reconnect_delay).await;
                    //     // tokio::time::sleep(config.reconnect_delay).await;
                    // }
                    // #[cfg(feature = "async_std_runtime")]
                    // {
                    //     log::trace!("Using async std runtime");
                    //     async_std::task::sleep(config.reconnect_delay).await;
                    // }
                    log::trace!("Trying again");
                }
            }
        }
    }

    /// Declare exchange
    pub async fn declare_exchange(exchange: String, declare: DeclareExchange) {
        let this = Self::get();
        let this = this.lock().await;
        let mut exchanges = this.exchanges.write().await;
        exchanges.insert(exchange, declare);
    }



    /// Creates a channel. This will only return the channel if the connection is valid, otherwise will
    /// wait to get the connection, or fail for misconfigured connection.
    pub async fn create_channel() -> Arc<Channel> {
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
            let exchanges = this.exchanges.clone();
            drop(this);
            log::trace!("Running creator on rabbit");
            match creator(channel, exchanges).await {
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

    pub(crate) async fn create_exchange(channel: Arc<Channel>, exchange: &str) -> anyhow::Result<()> {
        let this = Self::get();
        let this = this.lock().await;
        let exchanges = this.exchanges.read().await;
        if let Some(declare) = exchanges.get(exchange) {
            declare(channel).await?;
        }
        Ok(())
    }
}
