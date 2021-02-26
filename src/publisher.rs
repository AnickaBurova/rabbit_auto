//! Sink wrapper for rabbit publisher.
use anyhow::{Result, Error};
use lapin::{BasicProperties, Channel};

use core::pin::Pin;
use futures::{
    future::Future,
    task::{Context, Poll},
    Sink,
};
use std::collections::VecDeque;
use crate::comms::Comms;
use lapin::options::{ConfirmSelectOptions, BasicPublishOptions};

/// Callback to create confirm select options
pub type GetCSO = Pin<Box<dyn Fn() -> ConfirmSelectOptions + Send + Sync>>;
/// Callback to create basic publish options
pub type GetBPO = Pin<Box<dyn Fn() -> BasicPublishOptions + Send + Sync>>;
/// Callback to create basic publish properties
pub type GetBP = Pin<Box<dyn Fn() -> BasicProperties + Send + Sync>>;


type Data<E> = (Channel, E, Option<GetCSO>, Option<GetBPO>, Option<GetBP>);

/// Trait used to serialise the item in to byte vector data.
/// This is used as payload for the message.
pub trait Serialise {
    /// Gets the serialised data of the item.
    fn serialise(data: &Self) -> Vec<u8>;
}

/// Creates a callback which creates the simples case of confirm select options.
pub fn simple_confirm_options() -> GetCSO {
    Box::pin(|| lapin::options::ConfirmSelectOptions { nowait: false } )
}


enum State<Item, Address>
{
    Idle {
        channel: Channel,
        address: Address,
        confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
        queue: VecDeque<Item>,
    },
    Sending {
        send: Pin<Box<dyn Future<Output = Data<Address>> + Send>>,
        queue: VecDeque<Item>,
    }
}

/// Sink wrapper for rabbit publisher.
/// This will never fail after the initial successful creation.
pub struct PublishWrapper<Item, Address>
{
    state: Option<State<Item, Address>>,
}

impl<Item, Address> PublishWrapper<Item, Address> {
    async fn connect() -> Result<Channel> {
        Comms::create_channel().await
    }

    fn queue(&mut self, item: Item) -> Result<()> {
        match &mut self.state {
            Some(State::Idle { queue, .. }) => {
                queue.push_back(item);
            }
            Some(State::Sending { queue, .. }) => {
                queue.push_back(item);
            }
            None => anyhow::bail!("Invalid state"),
        }
        Ok(())
    }

    async fn set_channel(channel: Channel, confirm: Option<GetCSO>) -> std::result::Result<(Channel, Option<GetCSO>), (anyhow::Error, Option<GetCSO>)> {
        let confirm = if let Some(confirm) = confirm {
            if let Err(err) = channel
                .confirm_select(confirm())
                .await {
                return Err((anyhow::Error::from(err), Some(confirm)));
            }
            Some(confirm)
        } else {
            None
        };
        Ok((channel, confirm))
    }

    async fn send<T,E,K, D, R>(item: T,
                               mut channel: Channel,
                      exchange: E,
                      routing_key: K,
                      mut confirm: Option<GetCSO>,
                      publish_options: Option<GetBPO>,
                      publish_properties: Option<GetBP>,
                      return_func: R,
        ) -> Data<D>
            where T: Unpin + Send + Serialise,
                  E: AsRef<str> + Send + Unpin + 'static,
                  K: AsRef<str> + Send + Unpin + 'static,
                  R: FnOnce(Channel, E, K, Option<GetCSO>, Option<GetBPO>, Option<GetBP>) -> Data<D>,
        {
            loop {
                // item has to be serialised over and over, because the data is actually send over to rabbit.
                // but if the rabbit fails to pusblish it, the data itself is gone so we need to get a new one to repeat
                let data = Serialise::serialise(&item);
                let publish = channel.basic_publish(
                    exchange.as_ref(),
                    routing_key.as_ref(),
                    publish_options.as_ref().map(|o| o()).unwrap_or_else(||BasicPublishOptions::default()),
                    data,
                    publish_properties.as_ref().map(|p| p()).unwrap_or_else(|| BasicProperties::default()),
                );
                match publish.await {
                    Ok(result) => {
                        use lapin::publisher_confirm::Confirmation;
                        match (result.await, confirm.is_some()) {
                            (Ok(Confirmation::Ack(None)), true) => {
                                log::trace!(
                                    "Publish on channel {} confirmed",
                                    channel.id()
                                );
                                return return_func(channel, exchange, routing_key, confirm, publish_options, publish_properties);
                            }
                            (Ok(_), true) => {
                                log::error!("Failed to confirm message publish on channel {}, repeating the send", channel.id());
                                continue;
                            }
                            (Err(err), _) => {
                                log::error!("Failed to publish message on channel {}, repeating the send: {}", channel.id(), err);
                            }
                            (_, false) => {
                                log::trace!("Published");
                                return return_func(channel, exchange, routing_key, confirm, publish_options, publish_properties);
                            }
                        }
                    }
                    Err(err) => {
                        log::error!(
                            "Failed to send message to {} of key {}: {}",
                            exchange.as_ref(),
                            routing_key.as_ref(),
                            err
                        );
                    }
                }
                loop {
                    // here we can unwrap, because the connect should never fail if it succeeded once
                    match Self::set_channel(Self::connect().await.unwrap(), confirm).await {
                        Ok((value, con)) => {
                            channel = value;
                            confirm = con;
                            break;
                        }
                        Err((err, con)) => {
                            log::error!("Failing to set confirm select on the channel: {}", err);
                            confirm = con;
                        }
                    };
                }
            }
        }
    }

impl<T, E, K> Sink<T> for PublishWrapper<T, (E, K)>
    where T: Unpin + Send + Serialise + 'static,
          E: AsRef<str> + Send + Unpin + 'static,
          K: AsRef<str> + Send + Unpin + 'static,
{
    type Error = Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let this = Pin::into_inner(self);
        loop {
            match this.state.take() {
                Some(State::Sending { mut send, queue }) => {
                    let action = send.as_mut();
                    match Future::poll(action, cx) {
                        Poll::Pending => {
                            this.state = Some(State::Sending { send, queue });
                            return Poll::Pending;
                        }
                        Poll::Ready((channel, (exchange, routing_key), confirm, publish_options, publish_properties)) => {
                            // the data was send
                            this.state = Some(State::Idle { channel, address: (exchange, routing_key), queue, confirm, publish_options, publish_properties});
                        }
                    }
                }
                Some(State::Idle { channel, address: (exchange, routing_key), confirm, publish_options, publish_properties, mut queue }) => {
                    if let Some(item) = queue.pop_front() {
                        this.state = Some(State::Sending {
                            send: Box::pin(Self::send(item, channel, exchange, routing_key, confirm, publish_options, publish_properties, |channel, exchange, key, confirm, publish_options, publish_properties| (channel, (exchange, key), confirm, publish_options, publish_properties))),
                            queue,
                        });
                    } else {
                        this.state = Some(State::Idle { channel, address: (exchange, routing_key), queue, confirm, publish_options, publish_properties});
                        return Poll::Ready(Ok(()));
                    }
                }
                None => unreachable!(),
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<()> {
        let this = Pin::into_inner(self);
        this.queue(item)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Self::poll_ready(self, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Self::poll_ready(self, cx)
    }
}

impl<T, E, K> Sink<(T, K)> for PublishWrapper<(T, K), E>
    where T: Unpin + Send + Serialise + 'static,
          E: AsRef<str> + Send + Unpin + 'static,
          K: AsRef<str> + Send + Unpin + 'static,
{
    type Error = Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let this = Pin::into_inner(self);
        loop {
            match this.state.take() {
                Some(State::Sending { mut send, queue }) => {
                    let action = send.as_mut();
                    match Future::poll(action, cx) {
                        Poll::Pending => {
                            this.state = Some(State::Sending { send, queue });
                            return Poll::Pending;
                        }
                        Poll::Ready((channel, exchange, confirm, publish_options, publish_properties)) => {
                            // the data was send
                            this.state = Some(State::Idle { channel, address: exchange, queue, confirm, publish_options, publish_properties});
                        }
                    }
                }
                Some(State::Idle { channel, address: exchange, confirm, publish_options, publish_properties, mut queue }) => {
                    if let Some((item, routing_key)) = queue.pop_front() {
                        this.state  = Some(State::Sending {
                            send: Box::pin(Self::send(item, channel, exchange, routing_key, confirm, publish_options, publish_properties, |channel, exchange, _, confirm, publish_options, publish_properties| (channel, exchange, confirm, publish_options, publish_properties))),
                            queue,
                        });
                    } else {
                        this.state = Some(State::Idle { channel, address: exchange, queue, confirm, publish_options, publish_properties});
                        return Poll::Ready(Ok(()));
                    }
                }
                None => unreachable!(),
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: (T, K)) -> Result<()> {
        let this = Pin::into_inner(self);
        this.queue(item)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Self::poll_ready(self, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
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
        mut confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
    ) -> Result<Self>
    {
        loop {
            let (channel, confirm) = match Self::set_channel(Self::connect().await?, confirm).await {
                Ok(value) => value,
                Err((err, con)) => {
                    log::error!("Failing to set confirm select on the channel: {}", err);
                    confirm = con;
                    continue;
                }
            };
            return Ok(Self { state: Some(State::Idle { channel, address: (exchange, routing_key), confirm, publish_options, publish_properties, queue: VecDeque::new() } )});
        }
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
        mut confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
    ) -> Result<Self>
    {

        loop {
            let (channel, confirm) = match Self::set_channel(Self::connect().await?, confirm).await {
                Ok(value) => value,
                Err((err, con)) => {
                    log::error!("Failing to set confirm select on the channel: {}", err);
                    confirm = con;
                    continue;
                }
            };
            return Ok(Self { state: Some(State::Idle { channel, address: exchange, confirm, publish_options, publish_properties, queue: VecDeque::new() }) });
        }
    }
}

