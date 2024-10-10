//! Helper to create a connection and keep it connected.


use std::collections::HashMap;
use lapin::{Connection, Channel, ConnectionProperties, ConnectionState};
use std::sync::{Arc, Weak};
use futures::lock::Mutex;
use std::pin::Pin;
use futures::{Future};
use lapin::types::{AMQPValue, ShortUInt};
// use crate::comms::comms_data::Data;
use crate::config::Config;
use crate::exchanges::DeclareExchange;


pub const MAX_CHANNELS: usize = 16;
#[cfg(feature = "tokio_runtime")]
use tokio::sync::{mpsc::{Receiver, Sender,  channel}};


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
/// Internal size of the channel buffer
const COMMS_MSG_SIZE: usize = 32;

pub(crate) enum CommsMsg {
    DeclareExchange {
        exchange: String,
        declare: DeclareExchange,
    },
    RequestChannel(ChannelSender),
}


impl Comms {
    /// Declare exchange
    pub async fn declare_exchange(exchange: String, declare: DeclareExchange) {
        let sender = Self::channel_comms().0;
        log::debug!("Sending {exchange} declaration to the rabbit loop");
        if let Err(err) = sender.send(CommsMsg::DeclareExchange { exchange, declare }).await {
            log::error!("Declare exchange internal comms is broken: {err}");
            std::process::exit(-1);
        }
    }

    /// Gets the singleton global data
    /// The Receiver is taken away by the configure method when is started.
    fn channel_comms() -> (&'static Arc<Sender<CommsMsg>>, &'static Arc<Mutex<Option<Receiver<CommsMsg>>>>)
    {
        lazy_static::lazy_static! {
            static ref COMMS: (Arc<Sender<CommsMsg>>, Arc<Mutex<Option<Receiver<CommsMsg>>>>) = {
                let (tx, rx) = channel(COMMS_MSG_SIZE);
                (Arc::new(tx), Arc::new(Mutex::new(Some(rx))))
            };
        }
        (&COMMS.0, &COMMS.1)
    }


    /// Gets the singleton channel sender. This is already cloned and ready to use
    pub(crate) fn get_channel_comms() -> Arc<Sender<CommsMsg>> {
        Self::channel_comms().0.clone()
    }

    /// Creates a new channel to transfer Weak<Channel> object
    pub(crate) fn create_channel_channel() -> (Arc<Sender<Weak<Channel>>>, Receiver<Weak<Channel>>) {
        let (tx, rx) = channel(2);
        (Arc::new(tx), rx)
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
            // let (mut channel_requester, mut declare_exchange_requester)  = {
            let mut msg_receiver = {
                log::debug!("Taking the channel requester from the global area");
                let singleton = Self::channel_comms().1.clone();
                let mut channel_receiver = singleton.lock().await;
                if let Some(cr) = channel_receiver.take() {
                    cr
                } else {
                    log::error!("Fatal error. No channel receiver found. Is the Comms::configure called the second time?");
                    std::process::exit(1);
                }
            };
            log::debug!("Channel requester taken, we can start the rabbit loop");
            let mut connection: Option<Connection> = None;
            let mut connection_attempt = 0;
            let mut declare_exchanges = HashMap::new();

            let mut channels = channels_ring::Channels::new();
            let mut is_connecting = false;
            loop {
                let mut channel_sender = Vec::with_capacity(COMMS_MSG_SIZE);
                let mut msgs = Vec::with_capacity(COMMS_MSG_SIZE);
                let _ = msg_receiver.recv_many(&mut msgs, COMMS_MSG_SIZE).await;
                for msg in msgs.into_iter() {
                    match msg {
                        CommsMsg::DeclareExchange { exchange, declare } => {
                            log::trace!("Received a new exchange declaration for {exchange}");
                            declare_exchanges.insert(exchange, declare);
                        }
                        CommsMsg::RequestChannel(cs) => {
                            log::trace!("There is a request for a channel");
                            channel_sender.push(cs);
                        }
                    }
                }
                if channel_sender.is_empty() {
                    log::debug!("No channel requesters yet, going back waiting");
                    continue;
                }
                // try to establish a connection until we have one and can send a channel back to the requester
                loop {
                    // connect
                    if let Some(conn) = connection.take() {
                        // validate the connection
                        log::debug!("Validating an existing connection");
                        'connection: loop {
                            match conn.status().state() {
                                ConnectionState::Initial | ConnectionState::Connecting => {
                                    log::debug!("Connecting is in progress");
                                    // lets check again in a while
                                    config.reactor.sleep(std::time::Duration::from_millis(100)).await;
                                }
                                ConnectionState::Connected => {
                                    // the connection is valid
                                    if is_connecting {
                                        log::info!("Connection to RabbitMQ established");
                                        is_connecting = false;
                                    } else {
                                        log::debug!("RabbitMQ is already connected");
                                    }
                                    if !declare_exchanges.is_empty() {
                                        log::debug!("There are {} exchanges to declare", declare_exchanges.len());
                                        match channels.create_channel(&conn).await {
                                            Ok(channel) => {
                                                // run all exchange declarations
                                                while !declare_exchanges.is_empty() {
                                                    let mut done = None;
                                                    for (exchange, declare) in declare_exchanges.iter() {
                                                        log::debug!("Running exchange declaration for '{exchange}'");
                                                        if let Err(err) = declare(channel.clone()).await {
                                                            log::error!("Failed to declare an exchange: {err}");
                                                            continue 'connection;
                                                        }
                                                        log::debug!("Exchange '{exchange}' declared");
                                                        done = Some(exchange.clone());
                                                        break;
                                                    }
                                                    if let Some(exchange) = done {
                                                        declare_exchanges.remove(&exchange);
                                                    }
                                                }
                                            }
                                            Err(err) => {
                                                // creating the channel failed, we need a new connection
                                                log::error!("Connection to rabbit failed, reconnecting: {err}");
                                            }
                                        }
                                    }
                                    connection = Some(conn);
                                    log::debug!("Connection initialised");
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
                        log::debug!("There is no connection yet");
                    }
                    // check if the connection exists or we need a new one
                    if let Some(conn) = connection.take() {
                        // so the connection should be valid, we can finally get the channel and send it
                        log::debug!("Connection exist, getting channels");
                        match channels.create_channels(channel_sender.len(), &conn).await {
                            Ok(channels) => {
                                log::debug!("Channels are created, sending them to the individual requesters");
                                for (cs, channel) in channel_sender.drain(..).zip(channels) {
                                    let channel = Arc::downgrade(&channel);
                                    if let Err(err) = cs.send(channel).await {
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
                        log::debug!("Closing any existing channels if any");
                        channels.try_close().await; // close any existing channels

                        log::debug!("No connection, attempting to connect [{connection_attempt}]",);
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
                                log::info!("Connecting to RabbitMQ");
                                connection = Some(con);
                            }
                            Err(err) => {
                                log::error!("Failed to connect: {err}");
                                log::debug!("Sleeping for {} seconds", config.reconnect_delay.as_secs_f64());
                                config.reactor.sleep(config.reconnect_delay).await;
                            }
                        }
                    }
                }
            }
        }));
    }

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
        channel_requester: &Arc<Sender<CommsMsg>>,
    ) -> (Self::Object, Weak<Channel>) {
        let mut setup_channel = false;
        loop {
            // ensure we have some channel
            let channel = if let Some(channel) = channel.take() {
                channel
            } else {
                // if the channel is None, we need to request a new channel from the Comms
                if let Err(err) = channel_requester.send(CommsMsg::RequestChannel(channel_sender.clone())).await {
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