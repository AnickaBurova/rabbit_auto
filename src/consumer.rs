//! A stream wrapper for rabbitmq consumer. This stream never fails and will consume until stopped being used.
use core::pin::Pin;
#[cfg(feature = "tokio_runtime")]
use tokio::sync::mpsc::{Receiver, Sender};
// #[cfg(feature = "async_std_runtime")]
// use async_std::sync::RwLock;
use std::sync::{Arc, Weak};
use futures::{
    future::Future,
    stream::Stream,
    task::{Context, Poll},
};
use lapin::{message::Delivery, Channel, Consumer};
use anyhow::Result;
use crate::exchanges::DeclareExchange;

use super::comms::*;

/// Returns a future which creates the consumer from the provided channel.
type ConsumerCreator = Box<dyn RabbitDispatcher<Object = Consumer>>;

pub type CreatorResult<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;
pub type Creator<T> = Pin<Box<dyn Fn(Arc<Channel>, Option<Arc<DeclareExchange>>) -> CreatorResult<T> + Send + Sync>>;

pub type ChannelReceiver = Receiver<Weak<Channel>>;

type NextFuture = Pin<
    Box<
        dyn Future<
            Output = (
                Delivery,
                Data,
            ),
        > + Send,
    >,
>;

struct Data {
    /// RabbitMQ consumer
    consumer: Consumer,
    /// RabbitMQ channel, this has to be keep here for having the consumer alive, otherwise there will
    /// be non channel alive for the consumer, and the consumer would be dropped.
    channel: Weak<Channel>,
    /// Creator of the consumer
    creator: ConsumerCreator,
    /// This will be send to the Comms when the channel is not ok
    channel_sender: ChannelSender,
    /// Here will be a new channel delivered after requesting a new one
    channel_receiver: Receiver<Weak<Channel>>,
    /// The channel sender is requested over this
    channel_requester: Arc<Sender<CommsMsg>>,
}

enum State {
    /// Waiting to start looking for the next item
    Idle(Data),
    /// Looking for the next item
    Next {
        /// Future to get the next item
        next: NextFuture,
    }
}

/// Consumer wrapper handles errors in the connection. If the rabbitmq is disconnected, instead of
/// finishing the stream, the wrapper will try to reconnect and recreate the connection and continue
/// consuming like nothing happened. But if the connection was never established at least once, the stream
/// end right away!
pub struct ConsumerWrapper {
    state: Option<State>,
}

impl ConsumerWrapper {

    /// Create a new consumer by providing a creator function. This function might be called many times,
    /// as often as we loose connection to the rabbitmq.
    // pub async fn new(creator: ConsumerCreator) -> Result<Self> {
    //     let (creator, (channel, consumer)) = Self::connect(creator).await?;
    //     log::debug!("Consumer wrapper created");
    //     Ok(Self { state: Some(State::Idle { consumer, channel, creator }) })
    // }
    pub(crate) async fn new(creator: ConsumerCreator) -> Self {
        log::trace!("Getting channel requester");
        let channel_requester = Comms::get_channel_comms();
        let (channel_sender, mut channel_receiver) = Comms::create_channel_channel();
        log::trace!("Creating the consumer using the creator");
        let (consumer, channel) =
            creator.start_dispatch(
                None,
                &channel_sender,
                &mut channel_receiver,
                &channel_requester).await;
        log::trace!("Consumer wrapper created");
        Self {
            state: Some(State::Idle(Data { consumer, channel, creator, channel_sender, channel_receiver, channel_requester})),
        }
    }


    /// Gets the next item from the consumer. If the consumer is broken, then a new consumer is automatically created
    async fn next_item(mut data: Data)
        -> (Delivery, Data) {
        loop {
            use futures::stream::StreamExt;
            log::trace!("Polling consumer");
            match data.consumer.next().await {
                Some(Ok(delivery)) => {
                    log::trace!("Got delivery");
                    return (delivery, data);
                }
                Some(Err(err)) => {
                    log::error!("Failed to consume a message: {}", err);
                }
                None => {
                    log::error!("Consumer has finished for some reason!");
                }
            }
            log::warn!("Consumer is broken, waiting for a new connection");
            let (consumer, channel) = data.creator.start_dispatch(Some(data.channel.clone()),
                                                                  &data.channel_sender,
                                                                  &mut data.channel_receiver,
                                                                  &data.channel_requester).await;
            data.consumer = consumer;
            data.channel = channel;
        }
    }
}

impl Stream for ConsumerWrapper {
    type Item = Delivery;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        log::trace!("Poll next");
        let this = Pin::into_inner(self);

        loop {
            match this.state.take() {
                Some(State::Idle(data)) => {
                    this.state = Some(State::Next {
                        next: Box::pin(Self::next_item(data)),
                    });
                }
                Some(State::Next { mut next }) => {
                    let action = next.as_mut();
                    return match Future::poll(action, cx) {
                        Poll::Pending => {
                            this.state = Some(State::Next { next });
                            log::trace!("Pending");
                            Poll::Pending
                        }
                        Poll::Ready((delivery, data)) => {
                            this.state = Some(State::Idle(data));
                            log::trace!("Ready");
                            Poll::Ready(Some(delivery))
                        }
                    }
                }
                None => unreachable!(),
            }
        }
    }
}
