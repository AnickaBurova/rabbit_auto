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
use crate::comms::{ChannelSender, Comms, RabbitDispatcher};
use lapin::options::{ConfirmSelectOptions, BasicPublishOptions};
pub mod publisher_properties;
#[cfg(feature = "tokio_runtime")]
use tokio::sync::mpsc::{Receiver, UnboundedSender};

/// Callback to create confirm select options
pub type GetCSO = Pin<Box<dyn Fn() -> ConfirmSelectOptions + Send + Sync>>;
/// Callback to create basic publish options
pub type GetBPO = Pin<Box<dyn Fn() -> BasicPublishOptions + Send + Sync>>;
/// Callback to create basic publish properties
pub type GetBP = Pin<Box<dyn Fn() -> BasicProperties + Send + Sync>>;


// type Data<E> = (Weak<Channel>, E, Option<GetCSO>, Option<GetBPO>, Option<GetBP>);

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
    channel_requester: Arc<UnboundedSender<ChannelSender>>,
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

// impl<Item, RoutingKey, Exchange> ObjectCreator for Sender<Item, RoutingKey, Exchange> {
//
// }


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
        let mut this = Pin::into_inner(self);
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
        let mut this = Pin::into_inner(self);
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

// // NO LONGER IN USE
// struct ConfirmChannel {
//     confirm: GetCSO,
//     channel_sender: ChannelSender,
//     channel_receiver: Receiver<Weak<Channel>>,
//     channel_requester: Arc<UnboundedSender<ChannelSender>>,
// }
//
// impl ConfirmChannel {
//     async fn confirm_channel<I, A>(
//         mut self,
//         address: A,
//         publish_options: Option<GetBPO>,
//         publish_properties: Option<GetBP>,
//     ) -> PublishWrapper<I, A> {
//         let (_, channel) = self.create_object(
//             None,
//             &self.channel_sender,
//             &mut self.channel_receiver,
//             &self.channel_requester,
//         ).await;
//         PublishWrapper::<I,A>{
//             state: Some(
//                 State::Idle(IdleData::<I, A> {
//                     channel,
//                     address,
//                     confirm: Some(self.confirm),
//                     publish_options,
//                     publish_properties,
//                     queue: VecDeque::new(),
//                     channel_sender: self.channel_sender,
//                     channel_receiver: self.channel_receiver,
//                     channel_requester: self.channel_requester,
//                 })
//             )
//         }
//     }
// }
//
// struct PublisherInitialiser {
//     channel_sender: ChannelSender,
//     confirm: Option<GetCSO>,
//     channel_receiver: Receiver<Weak<Channel>>,
//     channel_requester: Arc<UnboundedSender<ChannelSender>>,
// }
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
        let channel_sender = Arc::new(channel_sender);
        PublishWrapper::<I,A>{
            queue: VecDeque::new(),
            state: Some(
                State::Idle(IdleData::<A> {
                    channel: Weak::default(),
                    address: Some(address),
                    confirm,
                    publish_options,
                    publish_properties,
                    channel_sender: channel_sender,
                    channel_receiver: Some(channel_receiver),
                    channel_requester: channel_requester,
                })
            )
        }
    }
}

// impl ObjectCreator for ConfirmChannel {
//     type Object = ();
//
//     async fn creator(&self, channel: &Channel) -> Result<Self::Object> {
//         let _ = channel
//             .confirm_select(confirm())
//             .await?;
//         Ok(())
//     }
// }
//
//
// impl<Item, Exchange, RoutingKey> Sink<Item> for PublishWrapper<Item, (Exchange, RoutingKey)>
//     where Item: Unpin + Send + Serialise + 'static,
//           Exchange: AsRef<str> + Send + Unpin + 'static + Sync,
//           RoutingKey: AsRef<str> + Send + Unpin + 'static + Sync,
// {
//     type Error = Error;
//     fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
//         let this = Pin::into_inner(self);
//         loop {
//             match this.state.take() {
//                 Some(State::Sending { mut send, queue }) => {
//                     let action = send.as_mut();
//                     match Future::poll(action, cx) {
//                         Poll::Pending => {
//                             this.state = Some(State::Sending { send, queue });
//                             return Poll::Pending;
//                         }
//                         Poll::Ready((channel, (exchange, routing_key), confirm, publish_options, publish_properties)) => {
//                             // the data was send
//                             this.state = Some(State::Idle { channel, address: (exchange, routing_key), queue, confirm, publish_options, publish_properties});
//                         }
//                     }
//                 }
//                 Some(State::Idle(mut data)) => {
//                     // Some(State::Idle { channel, address: (exchange, routing_key), confirm, publish_options, publish_properties, mut queue }) => {
//                     if let Some(item) = data.queue.pop_front() {
//                         let return_func = move|channel, exchange, key, confirm, publish_options, publish_properties| (channel, (exchange, key), confirm, publish_options, publish_properties);
//                         this.state = Some(State::Sending {
//                             send: Box::pin(
//                                 Self::send(
//                                     item, channel, exchange, routing_key, confirm, publish_options, publish_properties, return_func)),
//                             queue,
//                         });
//                     } else {
//                         this.state = Some(State::Idle { channel, address: (exchange, routing_key), queue, confirm, publish_options, publish_properties});
//                         return Poll::Ready(Ok(()));
//                     }
//                 }
//                 None => unreachable!(),
//             }
//         }
//     }
//
//     fn start_send(self: Pin<&mut Self>, item: Item) -> Result<()> {
//         let this = Pin::into_inner(self);
//         this.queue(item)?;
//         Ok(())
//     }
//
//     fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
//         Self::poll_ready(self, cx)
//     }
//
//     fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
//         Self::poll_ready(self, cx)
//     }
// }
//
// impl<T, E, K> Sink<(T, K)> for PublishWrapper<(T, K), E>
//     where T: Unpin + Send + Serialise + 'static,
//           E: AsRef<str> + Send + Unpin + 'static + Sync,
//           K: AsRef<str> + Send + Unpin + 'static + Sync,
// {
//     type Error = Error;
//     fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
//         let this = Pin::into_inner(self);
//         loop {
//             match this.state.take() {
//                 Some(State::Sending { mut send, queue }) => {
//                     let action = send.as_mut();
//                     match Future::poll(action, cx) {
//                         Poll::Pending => {
//                             this.state = Some(State::Sending { send, queue });
//                             return Poll::Pending;
//                         }
//                         Poll::Ready((channel, exchange, confirm, publish_options, publish_properties)) => {
//                             // the data was send
//                             this.state = Some(State::Idle { channel, address: exchange, queue, confirm, publish_options, publish_properties});
//                         }
//                     }
//                 }
//                 Some(State::Idle { channel, address: exchange, confirm, publish_options, publish_properties, mut queue }) => {
//                     if let Some((item, routing_key)) = queue.pop_front() {
//                         this.state  = Some(State::Sending {
//                             send: Box::pin(Self::send(item, channel, exchange, routing_key, confirm, publish_options, publish_properties, |channel, exchange, _, confirm, publish_options, publish_properties| (channel, exchange, confirm, publish_options, publish_properties))),
//                             queue,
//                         });
//                     } else {
//                         this.state = Some(State::Idle { channel, address: exchange, queue, confirm, publish_options, publish_properties});
//                         return Poll::Ready(Ok(()));
//                     }
//                 }
//                 None => unreachable!(),
//             }
//         }
//     }
//
//     fn start_send(self: Pin<&mut Self>, item: (T, K)) -> Result<()> {
//         let this = Pin::into_inner(self);
//         this.queue(item)?;
//         Ok(())
//     }
//
//     fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
//         Self::poll_ready(self, cx)
//     }
//
//     fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
//         Self::poll_ready(self, cx)
//     }
// }

// trait TSender<Item, Address>: ObjectCreator {
//     type ItemData: Unpin + Send + Serialise;
//     type RoutingKey;
//     type Exchange;
//     type Data;
//     /// Check if there is an item to send and return appropriate state.
//     /// If there is no item in the queue, the first bool marks we can return a ready poll state.
//     fn start_sending(self, item: Item) -> SenderFuture<Address>;
//
//     fn pack_together(
//         routing_key: Self::RoutingKey,
//         exchange: Self::Exchange,
//         data: Self::Data,
//         channel: Weak<Channel>,
//         channel_sender: ChannelSender,
//         channel_receiver: Receiver<Weak<Channel>>,
//         channel_requester: Arc<UnboundedSender<ChannelSender>>,
//         ) -> Self;
//
//     fn send(
//         item: Self::ItemData,
//         routing_key: Self::RoutingKey,
//         exchange: Self::Exchange,
//         data: Self::Data,
//         channel: Weak<Channel>,
//         channel_sender: ChannelSender,
//         channel_receiver: Receiver<Weak<Channel>>,
//         channel_requester: Arc<UnboundedSender<ChannelSender>>,
//     ) -> SenderFuture<Address> {
//         Box::pin(async move {
//             let data = Self::ItemData::serialise(&item);
//             if data.as_ref().is_empty() {
//                 log::trace!("No data after serialisation of the item. Skipping sending an empty message");
//                 return Self::pack_together(routing_key, exchange, data, channel, channel_sender, channel_receiver, channel_requester);
//             }
//             loop {
//                 // get the channel
//                 let chan = if let Some(ch) = channel.upgrade() {
//
//                 } else {
//
//                 }
//             }
//         })
//     }
// }
//
// impl<Item, Address> PublishWrapper<Item, Address> {
// async fn connect() -> Arc<Channel> {
//     Comms::create_channel().await
// }

// fn queue(&mut self, item: Item) -> Result<()> {
//     match &mut self.state {
//         Some(State::Idle(IdleData { queue, .. })) => {
//             queue.push_back(item);
//         }
//         Some(State::Sending { queue, .. }) => {
//             queue.push_back(item);
//         }
//         None => anyhow::bail!("Invalid state"),
//     }
//     Ok(())
// }

// async fn set_channel(channel: &Weak<Channel>, confirm: Option<GetCSO>) -> std::result::Result< Option<GetCSO>, (anyhow::Error, Option<GetCSO>)> {
//         if let Some(confirm) = confirm {
//             if let Some(channel) = channel.upgrade() {
//                 if let Err(err) = channel
//                     .confirm_select(confirm())
//                     .await {
//                     Err((anyhow::Error::from(err), Some(confirm)))
//                 } else {
//                     Ok(Some(confirm))
//                 }
//             } else {
//                 Err((anyhow::anyhow!("Invalid channel"), Some(confirm)))
//             }
//         } else {
//             Ok(None)
//         }
// }

// async fn send<T, A, E,K, D, R>(
//         item: T,
//         exchange: E,
//         routing_key: K,
//         mut data: IdleData<T, A>,
//         return_func: R,
//     ) -> Data<D>
//         where T: Unpin + Send + Serialise,
//               A: Send + Sync + 'static,
//               E: AsRef<str> + Send + Unpin + 'static,
//               K: AsRef<str> + Send + Unpin + 'static,
//               R: FnOnce( E, K, IdleData<T, A>) -> Data<D>,
//     {
//         let data = T::serialise(&item);
//         if data.as_ref().is_empty()  {
//             log::trace!("Data is empty, skipping sending nothing");
//             return return_func( exchange, routing_key, data);
//         }
//         loop {
//             {
//                 let (publish, id) = {
//                     let channel = if let Some(ch) = channel.upgrade() {
//                         ch
//                     } else {
//                         let ch = Self::connect().await;
//                         channel = Arc::downgrade(&ch);
//                         ch
//                     };
//                     if let Err(err) = Comms::create_exchange(channel.clone(), exchange.as_ref()).await {
//                         log::error!("Error creating an exchange {:?}", err);
//                         std::process::exit(1);
//                     }
//                     // let exchanges = Comms::get_exchanges().await;
//                     // let exchanges = exchanges.read().await;
//                     (channel.basic_publish(
//                         exchange.as_ref(),
//                         routing_key.as_ref(),
//                         publish_options.as_ref().map(|o| o()).unwrap_or_else(|| BasicPublishOptions::default()),
//                         data.as_ref(),
//                         publish_properties.as_ref().map(|p| p()).unwrap_or_else(|| BasicProperties::default()),
//                     ).await, channel.id())
//                 };
//                 match publish {
//                     Ok(result) => {
//                         use lapin::publisher_confirm::Confirmation;
//                         match (result.await, confirm.is_some()) {
//                             (Ok(Confirmation::Ack(None)), true) => {
//                                 log::trace!(
//                                     "Publish on channel {} confirmed",
//                                     id,
//                                 );
//                                 return return_func(channel, exchange, routing_key, confirm, publish_options, publish_properties);
//                             }
//                             (Ok(_), true) => {
//                                 log::error!("Failed to confirm message publish on channel {}, repeating the send", id);
//                                 continue;
//                             }
//                             (Err(err), _) => {
//                                 log::error!("Failed to publish message on channel {}, repeating the send: {}", id, err);
//                             }
//                             (_, false) => {
//                                 log::trace!("Published");
//                                 return return_func(channel, exchange, routing_key, confirm, publish_options, publish_properties);
//                             }
//                         }
//                     }
//                     Err(err) => {
//                         log::error!(
//                             "Failed to send message to {} of key {}: {}",
//                             exchange.as_ref(),
//                             routing_key.as_ref(),
//                             err
//                         );
//                     }
//                 }
//             }
//             loop {
//                 // here we can unwrap, because the connect should never fail if it succeeded once
//                 match Self::set_channel(Self::connect().await, confirm).await {
//                     Ok((value, con)) => {
//                         channel = Arc::downgrade(&value);
//                         confirm = con;
//                         break;
//                     }
//                     Err((err, con)) => {
//                         log::error!("Failing to set confirm select on the channel: {}", err);
//                         confirm = con;
//                     }
//                 };
//             }
//         }
//     }
// }

// impl<Item, Address> Sender<Item, Address> for IdleData<Address> {
//     // fn start_sending(self, item: Item) -> SenderFuture<Address> {
//     //     todo!()
//     // }
// }

// struct SendItem<T, E, K> {
//     item: T,
//     exchange: E,
//     routing_key: K,
//     data: IdleData<T>,
// }



// #[async_trait::async_trait]
// impl<Item> ObjectCreator for IdleData<Item>
// impl<T, E, K> ObjectCreator for SendItem<T, E, K> {}
// where
//     Item: Sync + Send +'static,
//     Address: Sync + Send +'static,
// {
//     type Object = ();
//
//     async fn creator(&self, channel: &Channel) -> Result<Self::Object> {
//             (channel.basic_publish(
//                 exchange.as_ref(),
//                 routing_key.as_ref(),
//                 publish_options.as_ref().map(|o| o()).unwrap_or_else(|| BasicPublishOptions::default()),
//                 data.as_ref(),
//                 publish_properties.as_ref().map(|p| p()).unwrap_or_else(|| BasicProperties::default()),
//             ).await, channel.id())
//     }
// }
