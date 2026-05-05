//! Auto-reconnecting publisher ([`PublishWrapper`]) and supporting types.
use anyhow::Error;
use lapin::{BasicProperties, Channel};

use crate::comms::{ChannelSender, Comms, CommsMsg, RabbitDispatcher};
use core::pin::Pin;
use futures::{
    future::Future,
    task::{Context, Poll},
    Sink,
};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use std::collections::VecDeque;
use std::sync::{Arc, Weak};
pub mod publisher_properties;
use tokio::sync::mpsc::{Receiver, Sender as MpscSender};

/// Callback to create confirm select options
pub type GetCSO = Pin<Box<dyn Fn() -> ConfirmSelectOptions + Send + Sync>>;
/// Callback to create basic publish options
pub type GetBPO = Pin<Box<dyn Fn() -> BasicPublishOptions + Send + Sync>>;
/// Callback to create basic publish properties
pub type GetBP = Pin<Box<dyn Fn() -> BasicProperties + Send + Sync>>;

type SenderFuture<Address> = Pin<Box<dyn Future<Output = IdleData<Address>> + Send>>;

mod serialise;
pub use serialise::Serialise;

/// Returns a [`GetCSO`] callback that enables publisher confirms with `nowait = false`.
///
/// Pass this to [`PublishWrapper::with_static_key`] or [`PublishWrapper::with_dynamic_key`]
/// together with `wait_for_confirm: true` when you need delivery guarantees.
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::publisher::{PublishWrapper, Serialise, simple_confirm_options};
/// use std::borrow::Cow;
///
/// struct Msg(Vec<u8>);
/// impl Serialise for Msg {
///     fn serialise(data: &Self) -> Cow<'_, [u8]> { data.0.as_slice().into() }
/// }
///
/// # async fn example() {
/// let publisher = PublishWrapper::<Msg, _>::with_static_key(
///     "my-exchange",
///     "my.key",
///     true,                          // wait for broker ack
///     Some(simple_confirm_options()),
///     None,
///     None,
/// ).await;
/// # }
/// ```
pub fn simple_confirm_options() -> GetCSO {
    Box::pin(|| lapin::options::ConfirmSelectOptions { nowait: false })
}

struct IdleData<Address> {
    wait_for_confirm: bool,
    address: Option<Address>,
    channel: Weak<Channel>,
    confirm: Option<GetCSO>,
    publish_options: Option<GetBPO>,
    publish_properties: Option<GetBP>,
    /// This will be send to the Comms when the channel is not ok
    channel_sender: ChannelSender,
    /// Here will be a new channel delivered after requesting a new one
    channel_receiver: Option<Receiver<Weak<Channel>>>,
    /// The channel sender is requested over this
    channel_requester: Arc<MpscSender<CommsMsg>>,
}

trait StartSending<Item, Address> {
    fn start_sending(self, item: Item) -> State<Address>;
}

impl<Item, RoutingKey, Exchange> StartSending<(Item, RoutingKey), Exchange> for IdleData<Exchange>
where
    Item: Unpin + Send + Serialise + Sync + 'static,
    RoutingKey: Unpin + AsRef<str> + Send + Sync + 'static,
    Exchange: Unpin + AsRef<str> + Send + Sync + 'static,
{
    // type Item = (Item, RoutingKey);
    // type Address = Exchange;
    fn start_sending(mut self, item: (Item, RoutingKey)) -> State<Exchange>
where {
        State::Sending(Box::pin(async move {
            let (item, routing_key) = item;
            let mut channel_receiver = self.channel_receiver.take().unwrap();
            let exchange = self.address.take().unwrap();
            let sender = Sender {
                wait_for_confirm: self.wait_for_confirm,
                item,
                routing_key,
                exchange,
                confirm: self.confirm.take(),
                publish_options: self
                    .publish_options
                    .as_ref()
                    .map(|o| o())
                    .unwrap_or_else(|| BasicPublishOptions::default()),
                publish_properties: self
                    .publish_properties
                    .as_ref()
                    .map(|o| o())
                    .unwrap_or_else(|| BasicProperties::default()),
            };
            let (_, channel) = sender
                .start_dispatch(
                    Some(self.channel.clone()),
                    &self.channel_sender,
                    &mut channel_receiver,
                    &self.channel_requester,
                )
                .await;
            self.channel = channel;
            self.confirm = sender.confirm;
            self.address = Some(sender.exchange);
            self.channel_receiver = Some(channel_receiver);
            self
        }))
    }
}

impl<Item, Exchange, RoutingKey> StartSending<Item, (Exchange, RoutingKey)>
    for IdleData<(Exchange, RoutingKey)>
where
    Item: Unpin + Sync + Send + 'static + Serialise,
    Exchange: Unpin + AsRef<str> + Sync + Send + 'static,
    RoutingKey: Unpin + AsRef<str> + Sync + Send + 'static,
{
    fn start_sending(mut self, item: Item) -> State<(Exchange, RoutingKey)>
where {
        State::Sending(Box::pin(async move {
            let mut channel_receiver = self.channel_receiver.take().unwrap();
            let (exchange, routing_key) = self.address.take().unwrap();
            let sender = Sender {
                wait_for_confirm: self.wait_for_confirm,
                item,
                routing_key,
                exchange,
                confirm: self.confirm.take(),
                publish_options: self
                    .publish_options
                    .as_ref()
                    .map(|o| o())
                    .unwrap_or_else(|| BasicPublishOptions::default()),
                publish_properties: self
                    .publish_properties
                    .as_ref()
                    .map(|o| o())
                    .unwrap_or_else(|| BasicProperties::default()),
            };
            let (_, channel) = sender
                .start_dispatch(
                    Some(self.channel.clone()),
                    &self.channel_sender,
                    &mut channel_receiver,
                    &self.channel_requester,
                )
                .await;
            self.channel = channel;
            self.confirm = sender.confirm;
            self.address = Some((sender.exchange, sender.routing_key));
            self.channel_receiver = Some(channel_receiver);
            self
        }))
    }
}

enum State<Address> {
    Idle(IdleData<Address>),
    Sending(SenderFuture<Address>),
}

struct Sender<Item, RoutingKey, Exchange> {
    item: Item,
    routing_key: RoutingKey,
    exchange: Exchange,
    wait_for_confirm: bool,
    confirm: Option<GetCSO>,
    publish_options: BasicPublishOptions,
    publish_properties: BasicProperties,
}

#[async_trait::async_trait]
impl<Item, Exchange, RoutingKey> RabbitDispatcher for Sender<Item, RoutingKey, Exchange>
where
    Item: Unpin + Send + Serialise + Sync,
    RoutingKey: AsRef<str> + Send + Sync + Unpin + 'static,
    Exchange: AsRef<str> + Send + Sync + Unpin + 'static,
{
    type Object = ();
    async fn channel_setup(&self, channel: &Channel) -> anyhow::Result<()> {
        if let Some(confirm) = self.confirm.as_ref() {
            tracing::trace!("Setting up the confirm on the channel");
            channel.confirm_select(confirm()).await?;
        }
        Ok(())
    }
    async fn dispatcher(&self, channel: &Channel) -> anyhow::Result<()> {
        let data = Item::serialise(&self.item);
        if data.as_ref().is_empty() {
            tracing::trace!("Serialised data from the item are empty, skipping the publish");
            return Ok(());
        }
        loop {
            let publish = channel
                .basic_publish(
                    self.exchange.as_ref(),
                    self.routing_key.as_ref(),
                    self.publish_options,
                    data.as_ref(),
                    self.publish_properties.clone(),
                )
                .await?;
            if self.wait_for_confirm {
                use lapin::publisher_confirm::Confirmation;
                match (publish.await, self.confirm.is_some()) {
                    (Ok(Confirmation::Ack(None)), true) => {
                        tracing::trace!("Publish on the channel {} is confirmed", channel.id());
                        return Ok(());
                    }
                    (Ok(_), true) => {
                        tracing::error!("Failed to confirm the published message on the channel {}, repeating the send", channel.id());
                    }
                    (Err(err), _) => {
                        tracing::error!("Failed to publish the message on the channel {}, repeating the send: {err}", channel.id());
                    }
                    (_, false) => {
                        tracing::trace!("Published");
                        return Ok(());
                    }
                }
            } else {
                return Ok(());
            }
        }
    }
}

/// An auto-reconnecting RabbitMQ publisher that implements [`futures::Sink`].
///
/// Create one via [`PublishWrapper::with_static_key`] (routing key fixed at
/// construction) or [`PublishWrapper::with_dynamic_key`] (routing key supplied
/// with each message).
///
/// After the first successful connection the sink never surfaces a connection
/// error — it will block internally until the broker is reachable again, then
/// retry the in-flight message.
///
/// Messages that serialise to an empty byte slice (see [`Serialise`]) are
/// silently dropped.
///
/// # Type parameters
///
/// * `Item` — the message type; must implement [`Serialise`].
/// * `Address` — either `(Exchange, RoutingKey)` for static routing or just
///   `Exchange` for dynamic routing.  Use the constructors rather than spelling
///   this out by hand.
pub struct PublishWrapper<Item, Address> {
    queue: VecDeque<Item>,
    state: Option<State<Address>>,
}

impl<Item, Exchange, RoutingKey> Sink<(Item, RoutingKey)>
    for PublishWrapper<(Item, RoutingKey), Exchange>
where
    Item: Unpin + Send + Serialise + Sync + 'static,
    RoutingKey: AsRef<str> + Unpin + Send + Sync + 'static,
    Exchange: AsRef<str> + Unpin + Send + Sync + 'static,
{
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = Pin::into_inner(self);
        loop {
            match this.state.take() {
                Some(State::Sending(mut sender)) => {
                    let action = sender.as_mut();
                    match Future::poll(action, cx) {
                        Poll::Pending => {
                            tracing::trace!("Sending is still pending");
                            this.state = Some(State::Sending(sender));
                            return Poll::Pending;
                        }
                        Poll::Ready(data) => {
                            tracing::trace!("Sending is done");
                            this.state = Some(State::Idle(data));
                        }
                    }
                }
                Some(State::Idle(data)) => {
                    if let Some(item) = this.queue.pop_front() {
                        tracing::trace!(
                            "There is an item in the queue, sending it('{}')",
                            item.1.as_ref()
                        );
                        this.state = Some(data.start_sending(item));
                    } else {
                        tracing::trace!("No more item in the queue, idling");
                        this.state = Some(State::Idle(data));
                        return Poll::Ready(Ok(()));
                    }
                }

                None => unreachable!(),
            }
        }
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: (Item, RoutingKey),
    ) -> std::result::Result<(), Self::Error> {
        tracing::trace!(
            "Sending an item with a dynamic key ('{}'), pushing the item to the queue",
            item.1.as_ref()
        );
        let this = Pin::into_inner(self);
        this.queue.push_back(item);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Self::poll_ready(self, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Self::poll_ready(self, cx)
    }
}
impl<Item, Exchange, RoutingKey> Sink<Item> for PublishWrapper<Item, (Exchange, RoutingKey)>
where
    Item: Unpin + Sync + Send + 'static + Serialise,
    Exchange: Unpin + AsRef<str> + Sync + Send + 'static,
    RoutingKey: Unpin + AsRef<str> + Sync + Send + 'static,
{
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = Pin::into_inner(self);
        loop {
            match this.state.take() {
                Some(State::Sending(mut sender)) => {
                    let action = sender.as_mut();
                    match Future::poll(action, cx) {
                        Poll::Pending => {
                            tracing::trace!("Sending is still pending");
                            this.state = Some(State::Sending(sender));
                            return Poll::Pending;
                        }
                        Poll::Ready(data) => {
                            tracing::trace!("Sending is done");
                            this.state = Some(State::Idle(data));
                        }
                    }
                }
                Some(State::Idle(data)) => {
                    if let Some(item) = this.queue.pop_front() {
                        tracing::trace!("There is an item in the queue, sending it");
                        this.state = Some(data.start_sending(item));
                    } else {
                        tracing::trace!("No more item in the queue, idling");
                        this.state = Some(State::Idle(data));
                        return Poll::Ready(Ok(()));
                    }
                }

                None => unreachable!(),
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> std::result::Result<(), Self::Error> {
        tracing::trace!("Sending an item with a static key, pushing the item to the queue");
        let this = Pin::into_inner(self);
        this.queue.push_back(item);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Self::poll_ready(self, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Self::poll_ready(self, cx)
    }
}

impl<T, E, K> PublishWrapper<T, (E, K)>
where
    T: Unpin + Send + Serialise,
    E: AsRef<str> + Send + Unpin + 'static,
    K: AsRef<str> + Send + Unpin + 'static,
{
    /// Creates a publisher with a **fixed** routing key.
    ///
    /// The resulting sink accepts `Item` values directly — the routing key is
    /// bound at construction time and never changes.
    ///
    /// # Arguments
    ///
    /// * `exchange` — name of the exchange to publish to.
    /// * `routing_key` — routing key used for every message.
    /// * `wait_for_confirm` — when `true`, the publisher waits for a broker
    ///   `Ack` before completing each send.  Requires `confirm` to be `Some`.
    ///   Set to `false` for fire-and-forget publishing.
    /// * `confirm` — publisher-confirm options; use [`simple_confirm_options`]
    ///   or `None` to disable confirms entirely.
    /// * `publish_options` — AMQP basic-publish options; `None` uses defaults.
    /// * `publish_properties` — AMQP message properties (expiration, delivery
    ///   mode, …); `None` uses defaults.  See [`crate::publish_properties`] and
    ///   [`crate::publisher::publisher_properties`] for helpers.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rabbit_auto::publisher::{PublishWrapper, Serialise};
    /// use futures::SinkExt;
    /// use std::borrow::Cow;
    ///
    /// struct Event(String);
    /// impl Serialise for Event {
    ///     fn serialise(data: &Self) -> Cow<'_, [u8]> { data.0.as_bytes().into() }
    /// }
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let mut publisher = PublishWrapper::<Event, _>::with_static_key(
    ///     "events",
    ///     "user.created",
    ///     false, // fire-and-forget
    ///     None,
    ///     None,
    ///     None,
    /// ).await;
    ///
    /// publisher.send(Event("{}".to_string())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_static_key(
        exchange: E,
        routing_key: K,
        wait_for_confirm: bool,
        confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
    ) -> Self {
        tracing::trace!(
            "Creating a publisher on exchange {} with a static routing key {}",
            exchange.as_ref(),
            routing_key.as_ref()
        );
        PublisherInitialiser::create::<T, (E, K)>(
            (exchange, routing_key),
            wait_for_confirm,
            confirm,
            publish_options,
            publish_properties,
        )
    }
}

impl<T, E, K> PublishWrapper<(T, K), E>
where
    T: Unpin + Send + Serialise,
    E: AsRef<str> + Send + Unpin + 'static,
    K: AsRef<str> + Send + Unpin + 'static,
{
    /// Creates a publisher with a **per-message** routing key.
    ///
    /// The resulting sink accepts `(Item, RoutingKey)` tuples, so the routing
    /// key can differ for each message.
    ///
    /// All other arguments behave identically to [`PublishWrapper::with_static_key`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rabbit_auto::publisher::{PublishWrapper, Serialise};
    /// use futures::SinkExt;
    /// use std::borrow::Cow;
    ///
    /// struct Event(String);
    /// impl Serialise for Event {
    ///     fn serialise(data: &Self) -> Cow<'_, [u8]> { data.0.as_bytes().into() }
    /// }
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let mut publisher = PublishWrapper::<Event, _>::with_dynamic_key(
    ///     "events",
    ///     false,
    ///     None,
    ///     None,
    ///     None,
    /// ).await;
    ///
    /// publisher.send((Event("{}".to_string()), "user.created")).await?;
    /// publisher.send((Event("{}".to_string()), "order.placed")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_dynamic_key(
        exchange: E,
        wait_for_confirm: bool,
        confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
    ) -> Self {
        tracing::trace!(
            "Creating a publisher on exchange {} with a dynamic routing key",
            exchange.as_ref()
        );
        PublisherInitialiser::create::<(T, K), E>(
            exchange,
            wait_for_confirm,
            confirm,
            publish_options,
            publish_properties,
        )
    }
}

struct PublisherInitialiser;

impl PublisherInitialiser {
    fn create<I, A>(
        address: A,
        wait_for_confirm: bool,
        confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
    ) -> PublishWrapper<I, A> {
        tracing::trace!("Creating rabbit channel internals");
        let channel_requester = Comms::get_channel_comms();
        let (channel_sender, channel_receiver) = Comms::create_channel_channel();
        PublishWrapper::<I, A> {
            queue: VecDeque::new(),
            state: Some(State::Idle(IdleData::<A> {
                channel: Weak::default(),
                address: Some(address),
                wait_for_confirm,
                confirm,
                publish_options,
                publish_properties,
                channel_sender,
                channel_receiver: Some(channel_receiver),
                channel_requester,
            })),
        }
    }
}
