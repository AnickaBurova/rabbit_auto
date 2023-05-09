//! Helper to create a connection and keep it connected.


use std::collections::HashMap;
use lapin::{Connection, Channel, ConnectionProperties, ConnectionState};
use std::sync::{Arc, Weak};
use futures::lock::Mutex;
use std::pin::Pin;
use futures::Future;
use lapin::types::{AMQPValue, ShortUInt};
// use crate::comms::comms_data::Data;
use crate::config::Config;
use crate::exchanges::DeclareExchange;


pub const MAX_CHANNELS: usize = 16;
#[cfg(feature = "tokio_runtime")]
use tokio::sync::{mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, unbounded_channel, channel}};


#[cfg(feature = "async_std_runtime")]
use async_std::sync::RwLock;
use executor_trait::FullExecutor;
use reactor_trait::Reactor;

/// Result of the creator function
pub type CreatorResult<T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send>>;
/// Creator function
pub type Creator<T> = Pin<Box<dyn Fn(Arc<Channel>, Option<Arc<DeclareExchange>>) -> CreatorResult<T> + Send + Sync>>;

pub type ChannelSender = Arc<Sender<Weak<Channel>>>;


// mod comms_data;

/// RabbitMQ connection singleton
pub struct Comms;
// {
//     /// Login and config data
//     config: Option<Config>,
//     /// Counter
//     connection_attempt: usize,
//     // exchanges: Arc<RwLock<HashMap<String, DeclareExchange>>>,
//     exchanges: HashMap<String, Arc<DeclareExchange>>,
//     /// The connection
//     connection: Option<Data>,
//     // should_sleep: bool,
// }


impl Comms {
    /// Declare exchange
    pub async fn declare_exchange(exchange: String, declare: DeclareExchange) {
        let sender = Self::channel_comms().1;
        if let Err(err) = sender.send((exchange, declare)) {
            log::error!("Declare exchange internal comms is broken: {err}");
            std::process::exit(-1);
        }
        // let this = Self::get();
        // let mut this = this.lock().await;
        // if this.exchanges.contains_key(&exchange) {
        //     anyhow::bail!("Exchange {} already declared", exchange);
        // }
        // // let mut exchanges = this.exchanges.write().await;
        // this.exchanges.insert(exchange, Arc::new(declare));
        // Ok(())
    }

    /// Gets the singleton global data
    fn channel_comms() -> (
        &'static Arc<UnboundedSender<ChannelSender>>,
        &'static Arc<UnboundedSender<(String, DeclareExchange)>>,
        &'static Arc<Mutex<Option<(
            UnboundedReceiver<ChannelSender>,
            UnboundedReceiver<(String, DeclareExchange)>,
        )>>>
    ){
        lazy_static::lazy_static! {
            static ref CHANNEL_COMMS:
                (Arc<UnboundedSender<ChannelSender>>,
                    Arc<UnboundedSender<(String, DeclareExchange)>>,
                    Arc<Mutex<Option<(UnboundedReceiver<ChannelSender>, UnboundedReceiver<(String, DeclareExchange)>)>>>) = {
                    let (tx, rx) = unbounded_channel();
                let (dtx, drx) = unbounded_channel();
                    (Arc::new(tx),
                        Arc::new(dtx),
                        Arc::new(Mutex::new(Some((rx, drx)))))
                };
        }
        (&CHANNEL_COMMS.0, &CHANNEL_COMMS.1, &CHANNEL_COMMS.2)
    }


    /// Gets the singleton channel sender. This is already cloned and ready to use
    pub(crate) fn get_channel_comms() -> Arc<UnboundedSender<ChannelSender>> {
        Self::channel_comms().0.clone()
    }

    /// Creates a new channel to transfer Weak<Channel> object
    pub(crate) fn create_channel_channel() -> (Sender<Weak<Channel>>, Receiver<Weak<Channel>>) {
        channel(2)
    }


    /// The connection needs to be configured before the first using
    /// This is no longer async method and this will start the rabbit auto connection manager.
    pub fn configure<E, R>(config: Config<E, R>)
        where
            E: FullExecutor + Sync + Send + 'static,
            R: Reactor + Sync + Send + 'static,
    {
        let executor = config.executor.clone();
        executor.spawn(Box::pin(async move {
            let (mut channel_requester, mut declare_exchange_requester)  = {
                log::trace!("Taking the channel requester from the global area");
                let singleton = Self::channel_comms().2.clone();
                let mut channel_receiver = singleton.lock().await;
                if let Some(cr) = channel_receiver.take() {
                    cr
                } else {
                    log::error!("Fatal error. No channel receiver found. Is the Comms::configure called the second time?");
                    std::process::exit(1);
                }
            };
            log::trace!("Channel requester taken, we can start the rabbit loop");
            let mut connection: Option<Connection> = None;
            let mut connection_attempt = 0;
            let mut declare_exchanges = HashMap::new();

            let mut channels = channels_ring::Channels::new();
            loop {
                let mut channel_sender = None;
                tokio::select!(
                    Some((exchange, declare)) = declare_exchange_requester.recv() => {
                        log::trace!("Received a new exchange declaration for {exchange}");
                        declare_exchanges.insert(exchange, declare);
                    }
                    Some(cs) = channel_requester.recv() => {
                        log::trace!("There is a request for a channel");
                        channel_sender = Some(cs);
                    }
                );
                // try to establish a connection until we have one and can send a channel back to the requester
                loop {
                    // connect
                    if let Some(conn) = connection.take() {
                        // validate the connection
                        log::trace!("Connection exist, validating it");
                        'connection: loop {
                            match conn.status().state() {
                                ConnectionState::Initial | ConnectionState::Connecting => {
                                    log::trace!("Connecting is in progress");
                                    // lets check again in a while
                                    config.reactor.sleep(std::time::Duration::from_millis(100)).await;
                                }
                                ConnectionState::Connected => {
                                    // the connection is valid
                                    log::info!("Rabbit is connected");
                                    if !declare_exchanges.is_empty() {
                                        match channels.create_channel(&conn).await.map(|wc| wc.upgrade()) {
                                            Ok(Some(channel)) => {
                                                // run all exchange declarations
                                                for (exchange, declare) in declare_exchanges.iter() {
                                                    log::trace!("Running exchange declaration for '{exchange}'");
                                                    if let Err(err) = declare(channel.clone()).await {
                                                        log::error!("Failed to declare an exchange: {err}");
                                                        continue 'connection;
                                                    }
                                                }
                                            }
                                            Ok(None) => {
                                                log::error!("Channel is already gone, this should be unreachable");
                                                std::process::exit(1);
                                            }
                                            Err(err) => {
                                                // creating the channel failed, we need a new connection
                                                log::error!("Connection to rabbit failed, reconnecting: {err}");
                                            }
                                        }
                                    }
                                    connection = Some(conn);
                                    break;
                                }
                                ConnectionState::Closing |
                                ConnectionState::Closed |
                                ConnectionState::Error => {
                                    log::warn!("Failed to connect to the rabbit server, will try to reconnect");
                                    log::trace!("Sleeping for {} seconds", config.reconnect_delay.as_secs_f64());
                                    config.reactor.sleep(config.reconnect_delay).await;
                                    break;
                                }
                            }
                        }
                    } else {
                        log::trace!("There is no connection yet");
                    }
                    // check if the connection exists or we need a new one
                    if let Some(conn) = connection.take() {
                        // so the connection should be valid, we can finally get the channel and send it
                        log::trace!("Connection exist, getting the channel");
                        match channels.create_channel(&conn).await {
                            Ok(channel) => {
                                log::trace!("Channel created, sending it to the requester");
                                if let Some(channel_sender) = channel_sender.as_ref() {
                                    if let Err(err) = channel_sender.send(channel).await {
                                        log::error!("Fatal error. Failed to send the channel back to the requester: {err}");
                                        std::process::exit(1);
                                    }
                                }
                                connection = Some(conn);
                                break;
                            }
                            Err(err) => {
                                // creating the channel failed, we need a new connection
                                log::error!("Connection to rabbit failed, reconnecting: {err}");
                            }
                        }
                    } else {
                        // we need a new one
                        log::trace!("Closing any existing channels if any");
                        channels.try_close().await; // close any existing channels

                        log::trace!("No connection, attempting to connect [{connection_attempt}]",);
                        let mut properties = ConnectionProperties::default()
                            .with_connection_name(config.name.clone().into());
                        properties.executor = Some(config.executor.clone());
                        properties.reactor = Some(config.reactor.clone());
                        properties
                            .client_properties
                            .insert("channel_max".into(), AMQPValue::ShortUInt(MAX_CHANNELS as ShortUInt));
                        let addr = config.address[connection_attempt % config.address.len()].as_ref();
                        connection_attempt += 1;
                        let con = Connection::connect(addr, properties).await;
                        match con {
                            Ok(con) => {
                                // we have a new connection, we will wait until fully connected
                                log::info!("Rabbit is connecting");
                                connection = Some(con);
                            }
                            Err(err) => {
                                log::error!("Failed to connect: {err}");
                                log::trace!("Sleeping for {} seconds", config.reconnect_delay.as_secs_f64());
                                config.reactor.sleep(config.reconnect_delay).await;
                            }
                        }
                    }
                }
            }
        }));
    }

    // /// Gets the Comms's Singleton
    // pub fn get() -> Arc<Mutex<Self>> {
    //     lazy_static::lazy_static! {
    //         static ref SINGLETON: Arc<Mutex<Comms>> = Comms::new();
    //     }
    //
    //     SINGLETON.clone()
    // }

    // pub(crate) async fn create_exchange(channel: Arc<Channel>, exchange: &str) -> anyhow::Result<()> {
    //     let this = Self::get();
    //     let this = this.lock().await;
    //     if let Some(declare) = this.exchanges.get(exchange) {
    //         declare(channel).await?;
    //     }
    //     Ok(())
    // }
}

/// This will dispatch an operation on a channel. If the channel is not
/// valid, it will request a new channel and when all is ok, it will dispatch the operation.
#[async_trait::async_trait]
pub(crate) trait RabbitDispatcher: Sync + Send {
    type Object;
    /// This can only fail if the rabbit connection is broken, otherwise it will get into a dead lock
    async fn dispatcher(&self, channel: &Channel) -> anyhow::Result<Self::Object>;
    async fn channel_setup(&self, _channel: &Channel) -> anyhow::Result<()> {
        Ok(())
    }
    async fn start_dispatch(
        &self,
        mut channel: Option<Weak<Channel>>,
        channel_sender: &ChannelSender,
        channel_receiver: &mut Receiver<Weak<Channel>>,
        channel_requester: &Arc<UnboundedSender<ChannelSender>>,
    ) -> (Self::Object, Weak<Channel>) {
        let mut setup_channel = false;
        loop {
            // ensure we have some channel
            let channel = if let Some(channel) = channel.take() {
                channel
            } else {
                // if the channel is None, we need to request a new channel from the Comms
                if let Err(err) = channel_requester.send(channel_sender.clone()) {
                    log::error!("Fatal internal error. Failed to request a new channel: {err}");
                    std::process::exit(1);
                }
                // now eventually the Channel should arrive over the channel_receiver
                if let Some(channel) =  channel_receiver.recv().await {
                    log::trace!("Channel received");
                    setup_channel = true;
                    channel
                } else {
                    log::error!("Fatal internal error. Failed to receive a new channel");
                    std::process::exit(1);
                }
            };
            // ensure the channel is alive, if not, get a new one
            let channel = match channel.upgrade() {
                None => {
                    log::trace!("No channel, we are going to wait for a new one");
                    continue;
                }
                Some(channel) => {
                  channel
                }
            };

            if setup_channel {
                log::trace!("Setting up the channel");
                if let Err(err) = self.channel_setup(&channel).await {
                    log::error!("Failed to setup the channel: {err}");
                    continue;
                }
                log::trace!("Channel setup done");
            }

            log::trace!("Dispatching the operation");
            match self.dispatcher(channel.as_ref()).await {
                Ok(object) => {
                    log::trace!("Operation is dispatched");
                    return (object, Arc::downgrade(&channel));
                }
                Err(err) => {
                    log::error!("Failed to failed to dispatch the operation: {}", err);
                    continue;
                }
            }
        }
    }
}

mod channels_ring;