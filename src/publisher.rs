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
use std::sync::{Arc, Weak};
use crate::comms::Comms;
use lapin::options::{ConfirmSelectOptions, BasicPublishOptions};
pub mod publisher_properties;

/// Callback to create confirm select options
pub type GetCSO = Pin<Box<dyn Fn() -> ConfirmSelectOptions + Send + Sync>>;
/// Callback to create basic publish options
pub type GetBPO = Pin<Box<dyn Fn() -> BasicPublishOptions + Send + Sync>>;
/// Callback to create basic publish properties
pub type GetBP = Pin<Box<dyn Fn() -> BasicProperties + Send + Sync>>;


type Data<E> = (Weak<Channel>, E, Option<GetCSO>, Option<GetBPO>, Option<GetBP>);

mod serialise;
pub use serialise::Serialise;

/// Creates a callback which creates the simples case of confirm select options.
pub fn simple_confirm_options() -> GetCSO {
    Box::pin(|| lapin::options::ConfirmSelectOptions { nowait: false } )
}


enum State<Item, Address>
{
    Idle {
        channel: Weak<Channel>,
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
    async fn connect() -> Arc<Channel> {
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

    async fn set_channel(channel: Arc<Channel>, confirm: Option<GetCSO>) -> std::result::Result<(Arc<Channel>, Option<GetCSO>), (anyhow::Error, Option<GetCSO>)> {
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

    async fn send<T, E,K, D, R>(
            item: T,
            mut channel: Weak<Channel>,
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
                  R: FnOnce(Weak<Channel>, E, K, Option<GetCSO>, Option<GetBPO>, Option<GetBP>) -> Data<D>,
        {
            let data = T::serialise(&item);
            if data.as_ref().is_empty()  {
                log::trace!("Data is empty, skipping sending nothing");
                return return_func(channel, exchange, routing_key, confirm, publish_options, publish_properties);
            }
            loop {
                {
                    let (publish, id) = {
                        let channel = if let Some(ch) = channel.upgrade() {
                            ch
                        } else {
                            let ch = Self::connect().await;
                            channel = Arc::downgrade(&ch);
                            ch
                        };
                        if let Err(err) = Comms::create_exchange(channel.clone(), exchange.as_ref()).await {
                            log::error!("Error creating an exchange {:?}", err);
                            std::process::exit(1);
                        }
                        // let exchanges = Comms::get_exchanges().await;
                        // let exchanges = exchanges.read().await;
                        (channel.basic_publish(
                            exchange.as_ref(),
                            routing_key.as_ref(),
                            publish_options.as_ref().map(|o| o()).unwrap_or_else(|| BasicPublishOptions::default()),
                            data.as_ref(),
                            publish_properties.as_ref().map(|p| p()).unwrap_or_else(|| BasicProperties::default()),
                        ).await, channel.id())
                    };
                    match publish {
                        Ok(result) => {
                            use lapin::publisher_confirm::Confirmation;
                            match (result.await, confirm.is_some()) {
                                (Ok(Confirmation::Ack(None)), true) => {
                                    log::trace!(
                                        "Publish on channel {} confirmed",
                                        id,
                                    );
                                    return return_func(channel, exchange, routing_key, confirm, publish_options, publish_properties);
                                }
                                (Ok(_), true) => {
                                    log::error!("Failed to confirm message publish on channel {}, repeating the send", id);
                                    continue;
                                }
                                (Err(err), _) => {
                                    log::error!("Failed to publish message on channel {}, repeating the send: {}", id, err);
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
                }
                loop {
                    // here we can unwrap, because the connect should never fail if it succeeded once
                    match Self::set_channel(Self::connect().await, confirm).await {
                        Ok((value, con)) => {
                            channel = Arc::downgrade(&value);
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
          E: AsRef<str> + Send + Unpin + 'static + Sync,
          K: AsRef<str> + Send + Unpin + 'static + Sync,
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
                        let return_func = move|channel, exchange, key, confirm, publish_options, publish_properties| (channel, (exchange, key), confirm, publish_options, publish_properties);
                        this.state = Some(State::Sending {
                            send: Box::pin(
                                Self::send(
                                    item, channel, exchange, routing_key, confirm, publish_options, publish_properties, return_func)),
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
          E: AsRef<str> + Send + Unpin + 'static + Sync,
          K: AsRef<str> + Send + Unpin + 'static + Sync,
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
    ) -> Self
    {
        loop {
            let (channel, confirm) = match Self::set_channel(Self::connect().await, confirm).await {
                Ok(value) => value,
                Err((err, con)) => {
                    log::error!("Failing to set confirm select on the channel: {}", err);
                    confirm = con;
                    continue;
                }
            };
            return Self { state: Some(State::Idle { channel: Arc::downgrade(&channel), address: (exchange, routing_key), confirm, publish_options, publish_properties, queue: VecDeque::new() } )};
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
    ) -> Self
    {

        loop {
            let (channel, confirm) = match Self::set_channel(Self::connect().await, confirm).await {
                Ok(value) => value,
                Err((err, con)) => {
                    log::error!("Failing to set confirm select on the channel: {}", err);
                    confirm = con;
                    continue;
                }
            };
            return Self { state: Some(State::Idle { channel: Arc::downgrade(&channel), address: exchange, confirm, publish_options, publish_properties, queue: VecDeque::new() }) };
        }
    }
}

