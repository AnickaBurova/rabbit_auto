//! Sink wrapper for rabbit publisher.
use anyhow::{Error};
use lapin::{BasicProperties, Channel};

use core::pin::Pin;
use futures::{
    future::Future,
    task::{Context, Poll},
    Sink,
};
use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use crate::comms::{ChannelSender, Comms, CommsMsg, RabbitDispatcher};
use lapin::options::{ConfirmSelectOptions, BasicPublishOptions};
pub mod publisher_properties;
#[cfg(feature = "tokio_runtime")]
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

/// Creates a callback which creates the simples case of confirm select options.
pub fn simple_confirm_options() -> GetCSO {
    Box::pin(|| lapin::options::ConfirmSelectOptions { nowait: false } )
}

struct IdleData<Address> {
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
    where
    {
        State::Sending(Box::pin(async move {
            let (item, routing_key) = item;
            let mut channel_receiver = self.channel_receiver.take().unwrap();
            let exchange = self.address.take().unwrap();
            let sender = Sender {
                item,
                routing_key,
                exchange,
                confirm: self.confirm.take(),
                publish_options: self.publish_options.as_ref().map(|o| o()).unwrap_or_else(|| BasicPublishOptions::default()),
                publish_properties: self.publish_properties.as_ref().map(|o|o()).unwrap_or_else(|| BasicProperties::default()),
            };
            let (_, channel) = sender.start_dispatch(
                Some(self.channel.clone()),
                &self.channel_sender,
                &mut channel_receiver,
                &self.channel_requester
            ).await;
            self.channel = channel;
            self.confirm = sender.confirm;
            self.address = Some(sender.exchange);
            self.channel_receiver = Some(channel_receiver);
            self
        }))
    }
}

impl<Item, Exchange, RoutingKey> StartSending<Item, (Exchange, RoutingKey)> for IdleData<(Exchange, RoutingKey)>
    where
        Item: Unpin + Sync + Send + 'static + Serialise,
        Exchange: Unpin + AsRef<str> + Sync + Send + 'static,
        RoutingKey: Unpin + AsRef<str> + Sync + Send + 'static,
{
    fn start_sending(mut self, item: Item) -> State<(Exchange, RoutingKey)>
        where
    {
        State::Sending(Box::pin(async move {
            let mut channel_receiver = self.channel_receiver.take().unwrap();
            let (exchange, routing_key) = self.address.take().unwrap();
            let sender = Sender {
                item,
                routing_key,
                exchange,
                confirm: self.confirm.take(),
                publish_options: self.publish_options.as_ref().map(|o| o()).unwrap_or_else(|| BasicPublishOptions::default()),
                publish_properties: self.publish_properties.as_ref().map(|o|o()).unwrap_or_else(|| BasicProperties::default()),
            };
            let (_, channel) = sender.start_dispatch(
                Some(self.channel.clone()),
                &self.channel_sender,
                &mut channel_receiver,
                &self.channel_requester
            ).await;
            self.channel = channel;
            self.confirm = sender.confirm;
            self.address = Some((sender.exchange, sender.routing_key));
            self.channel_receiver = Some(channel_receiver);
            self
        }))
    }
}

enum State<Address>
{
    Idle(IdleData<Address>),
    Sending(SenderFuture<Address>),
}

struct Sender<Item, RoutingKey, Exchange> {
    item: Item,
    routing_key: RoutingKey,
    exchange: Exchange,
    confirm: Option<GetCSO>,
    publish_options: BasicPublishOptions,
    publish_properties: BasicProperties,
}

#[async_trait::async_trait]
impl<Item, Exchange, RoutingKey> RabbitDispatcher for Sender<Item, RoutingKey, Exchange>
where
    Item: Unpin + Send + Serialise + Sync,
    RoutingKey: AsRef<str> + Send + Sync + Unpin + 'static,
    Exchange: AsRef<str> + Send + Sync + Unpin +'static,
{
    type Object = ();
    async fn channel_setup(&self, channel: &Channel) -> anyhow::Result<()> {
        if let Some(confirm) = self.confirm.as_ref() {
            log::trace!("Setting up the confirm on the channel");
            channel.confirm_select(confirm()).await?;
        }
        Ok(())
    }
    async fn dispatcher(&self, channel: &Channel) -> anyhow::Result<()> {
        let data = Item::serialise(&self.item);
        if data.as_ref().is_empty() {
            log::trace!("Serialised data from the item are empty, skipping the publish");
            return Ok(());
        }
        loop {
            let publish = channel.basic_publish(
                self.exchange.as_ref(),
                self.routing_key.as_ref(),
                self.publish_options,
                data.as_ref(),
                self.publish_properties.clone(),
            ).await?;
            use lapin::publisher_confirm::Confirmation;
            match (publish.await, self.confirm.is_some()) {
                (Ok(Confirmation::Ack(None)), true) => {
                    log::trace!("Publish on the channel {} is confirmed", channel.id());
                    return Ok(());
                }
                (Ok(_), true) => {
                    log::error!("Failed to confirm the published message on the channel {}, repeating the send", channel.id());
                }
                (Err(err), _) => {
                    log::error!("Failed to publish the message on the channel {}, repeating the send: {err}", channel.id());
                }
                (_, false) => {
                    log::trace!("Published");
                    return Ok(());
                }
            }
        }
    }
}



/// Sink wrapper for rabbit publisher.
/// This will never fail after the initial successful creation.
pub struct PublishWrapper<Item, Address>
{
    queue: VecDeque<Item>,
    state: Option<State<Address>>,
}


impl <Item, Exchange, RoutingKey> Sink<(Item, RoutingKey)> for PublishWrapper<(Item, RoutingKey), Exchange>
    where
        Item: Unpin + Send + Serialise + Sync + 'static,
        RoutingKey: AsRef<str> + Unpin + Send + Sync + 'static,
        Exchange: AsRef<str> + Unpin + Send + Sync + 'static,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        let this = Pin::into_inner(self);
        loop {
            match this.state.take() {
                Some(State::Sending(mut sender)) => {
                    let action = sender.as_mut();
                    match Future::poll(action, cx) {
                        Poll::Pending => {
                            log::trace!("Sending is still pending");
                            this.state = Some(State::Sending(sender));
                            return Poll::Pending;
                        }
                        Poll::Ready(data) => {
                            log::trace!("Sending is done");
                            this.state = Some(State::Idle(data));
                        }
                    }
                }
                Some(State::Idle(data)) => {
                    if let Some(item) = this.queue.pop_front() {
                        log::trace!("There is an item in the queue, sending it('{}')", item.1.as_ref());
                        this.state = Some(data.start_sending(item));
                    } else {
                        log::trace!("No more item in the queue, idling");
                        this.state = Some(State::Idle(data));
                        return Poll::Ready(Ok(()));
                    }
                }

                None => unreachable!(),
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: (Item, RoutingKey)) -> std::result::Result<(), Self::Error> {
        log::trace!("Sending an item with a dynamic key ('{}'), pushing the item to the queue", item.1.as_ref());
        let this = Pin::into_inner(self);
        this.queue.push_back(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Self::poll_ready(self, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Self::poll_ready(self, cx)
    }
}
impl <Item, Exchange, RoutingKey> Sink<Item> for PublishWrapper<Item,(Exchange, RoutingKey)>
where
    Item: Unpin + Sync + Send + 'static + Serialise,
    Exchange: Unpin + AsRef<str> + Sync + Send + 'static,
    RoutingKey: Unpin + AsRef<str> + Sync + Send + 'static,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        let this = Pin::into_inner(self);
        loop {
            match this.state.take() {
                Some(State::Sending(mut sender)) => {
                    let action = sender.as_mut();
                    match Future::poll(action, cx) {
                        Poll::Pending => {
                            log::trace!("Sending is still pending");
                            this.state = Some(State::Sending(sender));
                            return Poll::Pending;
                        }
                        Poll::Ready(data) => {
                            log::trace!("Sending is done");
                            this.state = Some(State::Idle(data));
                        }
                    }
                }
                Some(State::Idle(data)) => {
                    if let Some(item) = this.queue.pop_front() {
                        log::trace!("There is an item in the queue, sending it");
                        this.state = Some(data.start_sending(item));
                    } else {
                        log::trace!("No more item in the queue, idling");
                        this.state = Some(State::Idle(data));
                        return Poll::Ready(Ok(()));
                    }
                }

                None => unreachable!(),
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> std::result::Result<(), Self::Error> {
        log::trace!("Sending an item with a static key, pushing the item to the queue");
        let this = Pin::into_inner(self);
        this.queue.push_back(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Self::poll_ready(self, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Self::poll_ready(self, cx)
    }
}


impl<T,E,K> PublishWrapper<T, (E, K)>
    where T: Unpin + Send + Serialise,
          E: AsRef<str> + Send + Unpin + 'static,
          K: AsRef<str> + Send + Unpin + 'static,
{
    /// Creates a publisher with a static routing key.
    /// The routing key is provided during creation of the publisher and is static.
    /// The Sink is expecting only the item to publish.
    pub async fn with_static_key(
        exchange: E,
        routing_key: K,
        confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
    ) -> Self
    {
        log::trace!("Creating a publisher on exchange {} with a static routing key {}", exchange.as_ref(),
        routing_key.as_ref());
        PublisherInitialiser::create::<T, (E, K)>(
            ( exchange, routing_key),
            confirm,
            publish_options,
            publish_properties
        )
    }
}

impl<T,E,K> PublishWrapper<(T,K), E>
    where T: Unpin + Send + Serialise,
          E: AsRef<str> + Send + Unpin + 'static,
          K: AsRef<str> + Send + Unpin + 'static,
{

    /// Creates a publisher with a dynamic routing key.
    /// The Sink is expecting the item and the routing key.
    pub async fn with_dynamic_key(
        exchange: E,
        confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
    ) -> Self
    {
        log::trace!("Creating a publisher on exchange {} with a dynamic routing key", exchange.as_ref()
        );
        PublisherInitialiser::create::<(T, K), E>(
            exchange,
            confirm,
            publish_options,
            publish_properties
        )
    }
}

struct PublisherInitialiser;

impl PublisherInitialiser {
    fn create<I, A>(address: A,
                    confirm: Option<GetCSO>,
                    publish_options: Option<GetBPO>,
                    publish_properties: Option<GetBP>,
    ) -> PublishWrapper<I, A> {
        log::trace!("Creating rabbit channel internals");
        let channel_requester = Comms::get_channel_comms();
        let (channel_sender, channel_receiver) = Comms::create_channel_channel();
        PublishWrapper::<I,A>{
            queue: VecDeque::new(),
            state: Some(
                State::Idle(IdleData::<A> {
                    channel: Weak::default(),
                    address: Some(address),
                    confirm,
                    publish_options,
                    publish_properties,
                    channel_sender,
                    channel_receiver: Some(channel_receiver),
                    channel_requester,
                })
            )
        }
    }
}