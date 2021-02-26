//! A stream wrapper for rabbitmq consumer. This stream never fails and will consume until stopped being used.
use core::pin::Pin;
use futures::{
    future::Future,
    stream::Stream,
    task::{Context, Poll},
};
use lapin::{message::Delivery, Channel, Consumer};
use anyhow::Result;

use super::comms::*;

/// Returns a future which creates the consumer from the provided channel.
pub type ConsumerCreator = Creator<(Channel, Consumer)>;

type NextFuture = Pin<
    Box<
        dyn Future<
            Output = (
                (Channel, Delivery),
                Consumer,
                Channel,
                Creator<(Channel, Consumer)>,
            ),
        > + Send,
    >,
>;

enum State {
    /// Waiting to start looking for the next item
    Idle {
        /// RabbitMQ consumer
        consumer: Consumer,
        /// RabbitMQ channel, this has to be keep here for having the consumer alive, otherwise there will
        /// be non channel alive for the consumer, and the consumer would be dropped.
        channel: Channel,
        /// Creator of the consumer
        creator: ConsumerCreator,
    },
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
    pub async fn new(creator: ConsumerCreator) -> Result<Self> {
        let (creator, (channel, consumer)) = Self::connect(creator).await?;
        log::debug!("Consumer wrapper created");
        Ok(Self { state: Some(State::Idle { consumer, channel, creator }) })
    }

    /// Connects to the rabbit by passing the creator function
    async fn connect(
        creator: ConsumerCreator,
    ) -> Result<(ConsumerCreator, (Channel, Consumer))> {
        Comms::create_channel_and_object::<(Channel, Consumer)>(creator).await
    }


    /// Gets the next item from the consumer. If the consumer is broken, then a new consumer is automatically created
    async fn next_item(mut consumer: Consumer, mut channel: Channel, mut creator: ConsumerCreator)
        -> ((Channel, Delivery), Consumer, Channel, ConsumerCreator) {
        loop {
            use futures::stream::StreamExt;
            log::debug!("Polling consumer");
            match consumer.next().await {
                Some(Ok(delivery)) => {
                    log::debug!("Got delivery");
                    return (delivery, consumer, channel, creator);
                }
                Some(Err(err)) => {
                    log::error!("Failed to consume a message: {}", err);
                }
                None => {
                    log::error!("Consumer has finished for some reason!");
                }
            }
            log::warn!("Recreating the consumer");
            let (cr, (chan, cons)) = Self::connect(creator).await.unwrap();
            creator = cr;
            consumer = cons;
            channel = chan;
        }
    }
}

impl Stream for ConsumerWrapper {
    type Item = (Channel, Delivery);
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        log::debug!("Poll next");
        let this = Pin::into_inner(self);

        loop {
            match this.state.take() {
                Some(State::Idle { consumer, channel, creator }) => {
                    this.state = Some(State::Next {
                        next: Box::pin(Self::next_item(consumer, channel, creator)),
                    });
                }
                Some(State::Next { mut next }) => {
                    let action = next.as_mut();
                    return match Future::poll(action, cx) {
                        Poll::Pending => {
                            this.state = Some(State::Next { next });
                            log::debug!("Pending");
                            Poll::Pending
                        }
                        Poll::Ready((delivery, consumer, channel, creator)) => {
                            this.state = Some(State::Idle { consumer, channel, creator });
                            log::debug!("Ready");
                            Poll::Ready(Some(delivery))
                        }
                    }
                }
                None => unreachable!(),
            }
        }
    }
}
