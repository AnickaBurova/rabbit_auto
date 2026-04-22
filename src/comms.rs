//! Global connection manager ([`Comms`]) and the internal channel-dispatch machinery.

use async_rs::traits::{Executor, Reactor};
use async_rs::Runtime;
use futures::lock::Mutex;
use futures::Future;
use lapin::types::{AMQPValue, ShortUInt};
use lapin::{Channel, Connection, ConnectionProperties, ConnectionState};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Weak};
// use crate::comms::comms_data::Data;
use crate::config::Config;
use crate::exchanges::DeclareExchange;

pub const MAX_CHANNELS: usize = 16;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Result of the creator function
pub type CreatorResult<T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send>>;
/// Creator function
pub type Creator<T> =
    Pin<Box<dyn Fn(Arc<Channel>, Option<Arc<DeclareExchange>>) -> CreatorResult<T> + Send + Sync>>;

pub type ChannelSender = Arc<Sender<Weak<Channel>>>;

// mod comms_data;

/// Global RabbitMQ connection manager.
///
/// `Comms` is a process-wide singleton.  Call [`Comms::configure`] once at
/// startup to start the background connection loop, then use
/// [`Comms::declare_exchange`] to register any exchanges that consumers or
/// publishers depend on.
///
/// Consumers and publishers obtain AMQP channels through `Comms` automatically;
/// you do not interact with channels directly.
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
    /// Register an exchange declaration that will be (re-)run on every connection.
    ///
    /// Exchanges are declared once when the connection is first established and
    /// again automatically after every reconnect, so the topology is always in
    /// sync with the broker.
    ///
    /// Build the `declare` argument with one of the helpers in
    /// [`crate::exchanges`]: [`create_direct_exchange`](crate::exchanges::create_direct_exchange),
    /// [`create_topic_exchange`](crate::exchanges::create_topic_exchange), or
    /// the lower-level [`create_exchange`](crate::exchanges::create_exchange).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rabbit_auto::comms::Comms;
    /// use rabbit_auto::exchanges::create_topic_exchange;
    ///
    /// # async fn example() {
    /// Comms::declare_exchange(
    ///     "events".to_string(),
    ///     create_topic_exchange("events".to_string(), None, None),
    /// ).await;
    /// # }
    /// ```
    pub async fn declare_exchange(exchange: String, declare: DeclareExchange) {
        let sender = Self::channel_comms().0;
        tracing::debug!("Sending {exchange} declaration to the rabbit loop");
        if let Err(err) = sender
            .send(CommsMsg::DeclareExchange { exchange, declare })
            .await
        {
            tracing::error!("Declare exchange internal comms is broken: {err}");
            std::process::exit(-1);
        }
    }

    /// Gets the singleton global data
    /// The Receiver is taken away by the configure method when is started.
    fn channel_comms() -> (
        &'static Arc<Sender<CommsMsg>>,
        &'static Arc<Mutex<Option<Receiver<CommsMsg>>>>,
    ) {
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
    pub(crate) fn create_channel_channel() -> (Arc<Sender<Weak<Channel>>>, Receiver<Weak<Channel>>)
    {
        let (tx, rx) = channel(2);
        (Arc::new(tx), rx)
    }

    /// Start the background connection manager.
    ///
    /// Must be called **once** before creating any consumers or publishers.
    /// Calling it a second time is a fatal error (the process will exit).
    ///
    /// This is a synchronous function — it spawns the connection loop onto the
    /// async runtime and returns immediately.  The loop will keep retrying until
    /// the first connection is established, and will transparently reconnect
    /// whenever the connection is lost.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rabbit_auto::comms::Comms;
    /// use rabbit_auto::config::Config;
    /// use std::time::Duration;
    ///
    /// Comms::configure(Config::new(
    ///     "my-service".to_string(),
    ///     ["localhost:5672"].iter(),
    ///     "guest",
    ///     "guest",
    ///     Duration::from_secs(3),
    /// )).expect("Failed to initialise connection manager");
    /// ```
    pub fn configure(config: Config) -> anyhow::Result<()> {
        let rt = Runtime::tokio()?;
        let spawner = rt.clone();
        spawner.spawn(Box::pin(async move {
            // let (mut channel_requester, mut declare_exchange_requester)  = {
            let mut msg_receiver = {
                tracing::debug!("Taking the channel requester from the global area");
                let singleton = Self::channel_comms().1.clone();
                let mut channel_receiver = singleton.lock().await;
                if let Some(cr) = channel_receiver.take() {
                    cr
                } else {
                    tracing::error!("Fatal error. No channel receiver found. Is the Comms::configure called the second time?");
                    std::process::exit(1);
                }
            };
            tracing::debug!("Channel requester taken, we can start the rabbit loop");
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
                            tracing::trace!("Received a new exchange declaration for {exchange}");
                            declare_exchanges.insert(exchange, declare);
                        }
                        CommsMsg::RequestChannel(cs) => {
                            tracing::trace!("There is a request for a channel");
                            channel_sender.push(cs);
                        }
                    }
                }
                if channel_sender.is_empty() {
                    tracing::debug!("No channel requesters yet, going back waiting");
                    continue;
                }
                // try to establish a connection until we have one and can send a channel back to the requester
                loop {
                    // connect
                    if let Some(conn) = connection.take() {
                        // validate the connection
                        tracing::debug!("Validating an existing connection");
                        'connection: loop {
                            match conn.status().state() {
                                ConnectionState::Initial | ConnectionState::Connecting | ConnectionState::Reconnecting => {
                                    tracing::debug!("Connecting is in progress");
                                    // lets check again in a while
                                    rt.sleep(std::time::Duration::from_millis(100)).await;
                                }
                                ConnectionState::Connected => {
                                    // the connection is valid
                                    if is_connecting {
                                        tracing::info!("Connection to RabbitMQ established");
                                        is_connecting = false;
                                    } else {
                                        tracing::debug!("RabbitMQ is already connected");
                                    }
                                    if !declare_exchanges.is_empty() {
                                        tracing::debug!("There are {} exchanges to declare", declare_exchanges.len());
                                        match channels.create_channel(&conn).await {
                                            Ok(channel) => {
                                                // run all exchange declarations
                                                while !declare_exchanges.is_empty() {
                                                    let mut done = None;
                                                    for (exchange, declare) in declare_exchanges.iter() {
                                                        tracing::debug!("Running exchange declaration for '{exchange}'");
                                                        if let Err(err) = declare(channel.clone()).await {
                                                            tracing::error!("Failed to declare an exchange: {err}");
                                                            continue 'connection;
                                                        }
                                                        tracing::debug!("Exchange '{exchange}' declared");
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
                                                tracing::error!("Connection to rabbit failed, reconnecting: {err}");
                                            }
                                        }
                                    }
                                    connection = Some(conn);
                                    tracing::debug!("Connection initialised");
                                    break;
                                }
                                ConnectionState::Closing |
                                ConnectionState::Closed |
                                ConnectionState::Error => {
                                    tracing::warn!("Failed to connect to the rabbit server, will try to reconnect");
                                    tracing::trace!("Sleeping for {} seconds", config.reconnect_delay.as_secs_f64());
                                    rt.sleep(config.reconnect_delay).await;
                                    break;
                                }
                            }
                        }
                    } else {
                        tracing::debug!("There is no connection yet");
                    }
                    // check if the connection exists or we need a new one
                    if let Some(conn) = connection.take() {
                        // so the connection should be valid, we can finally get the channel and send it
                        tracing::debug!("Connection exist, getting channels");
                        match channels.create_channels(channel_sender.len(), &conn).await {
                            Ok(channels) => {
                                tracing::debug!("Channels are created, sending them to the individual requesters");
                                for (cs, channel) in channel_sender.drain(..).zip(channels) {
                                    let channel = Arc::downgrade(&channel);
                                    if let Err(err) = cs.send(channel).await {
                                        tracing::error!("Fatal error. Failed to send the channel back to the requester: {err}");
                                        std::process::exit(1);
                                    }
                                }
                                connection = Some(conn);
                                break;
                            }
                            Err(err) => {
                                // creating the channel failed, we need a new connection
                                tracing::error!("Connection to rabbit failed, reconnecting: {err}");
                            }
                        }
                    } else {
                        // we need a new one
                        tracing::debug!("Closing any existing channels if any");
                        channels.try_close().await; // close any existing channels

                        tracing::debug!("No connection, attempting to connect [{connection_attempt}]",);
                        let mut properties = ConnectionProperties::default()
                            .with_connection_name(config.name.clone().into());
                        properties
                            .client_properties
                            .insert("channel_max".into(), AMQPValue::ShortUInt(MAX_CHANNELS as ShortUInt));
                        let addr = config.address[connection_attempt % config.address.len()].as_ref();
                        connection_attempt += 1;
                        let con = Connection::connect(addr, properties).await;
                        match con {
                            Ok(con) => {
                                // we have a new connection, we will wait until fully connected
                                tracing::info!("Connecting to RabbitMQ");
                                connection = Some(con);
                            }
                            Err(err) => {
                                tracing::error!("Failed to connect: {err}");
                                tracing::debug!("Sleeping for {} seconds", config.reconnect_delay.as_secs_f64());
                                rt.sleep(config.reconnect_delay).await;
                            }
                        }
                    }
                }
            }
        }));
        Ok(())
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
                if let Err(err) = channel_requester
                    .send(CommsMsg::RequestChannel(channel_sender.clone()))
                    .await
                {
                    tracing::error!("Fatal internal error. Failed to request a new channel: {err}");
                    std::process::exit(1);
                }
                // now eventually the Channel should arrive over the channel_receiver
                if let Some(channel) = channel_receiver.recv().await {
                    tracing::trace!("Channel received");
                    setup_channel = true;
                    channel
                } else {
                    tracing::error!("Fatal internal error. Failed to receive a new channel");
                    std::process::exit(1);
                }
            };
            // ensure the channel is alive, if not, get a new one
            let channel = match channel.upgrade() {
                None => {
                    tracing::trace!("No channel, we are going to wait for a new one");
                    continue;
                }
                Some(channel) => channel,
            };

            if setup_channel {
                tracing::trace!("Setting up the channel");
                if let Err(err) = self.channel_setup(&channel).await {
                    tracing::error!("Failed to setup the channel: {err}");
                    continue;
                }
                tracing::trace!("Channel setup done");
            }

            tracing::trace!("Dispatching the operation");
            match self.dispatcher(channel.as_ref()).await {
                Ok(object) => {
                    tracing::trace!("Operation is dispatched");
                    return (object, Arc::downgrade(&channel));
                }
                Err(err) => {
                    tracing::error!("Failed to failed to dispatch the operation: {}", err);
                    continue;
                }
            }
        }
    }
}

mod channels_ring;
