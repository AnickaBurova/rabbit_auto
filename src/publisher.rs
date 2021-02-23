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

pub type GetCSO = Pin<Box<dyn Fn() -> ConfirmSelectOptions + Send + Sync>>;
pub type GetBPO = Pin<Box<dyn Fn() -> BasicPublishOptions + Send + Sync>>;
pub type GetBP = Pin<Box<dyn Fn() -> BasicProperties + Send + Sync>>;


type Data<E,K> = (Channel, E, K, Option<GetCSO>, Option<GetBPO>, Option<GetBP>);

pub trait Serialise {
    fn serialise(data: &Self) -> Vec<u8>;
}


/// Sink wrapper for rabbit publisher.
/// This will never fail after the initial successful creation.
pub enum PublishWrapper<T, E, K>
    where T: Unpin + Send + Serialise,
          E: AsRef<str> + Send + Unpin + 'static,
          K: AsRef<str> + Send + Unpin + 'static,
{
    Invalid,
    Idle {
        channel: Channel,
        exchange: E,
        routing_key: K,
        confirm: Option<GetCSO>,
        publish_options: Option<GetBPO>,
        publish_properties: Option<GetBP>,
        queue: VecDeque<T>,
    },
    Sending {
        send: Pin<Box<dyn Future<Output = Data<E,K>> + Send>>,
        queue: VecDeque<T>,
    }
}

impl<T, E, K> PublishWrapper<T, E, K>
    where T: Unpin + Send + Serialise,
          E: AsRef<str> + Send + Unpin + 'static,
          K: AsRef<str> + Send + Unpin + 'static,
{
    pub async fn new(exchange: E, routing_key: K, mut confirm: Option<GetCSO>, publish_options: Option<GetBPO>, publish_properties: Option<GetBP>) -> Result<Self> {
        loop {
            let (channel, confirm) = match Self::set_channel(Self::connect().await?, confirm).await {
                Ok(value) => value,
                Err((err, con)) => {
                    log::error!("Failing to set confirm select on the channel: {}", err);
                    confirm = con;
                    continue;
                }
            };
            return Ok(Self::Idle { channel, exchange, routing_key, confirm, publish_options, publish_properties, queue: VecDeque::new() });
        }
    }

    async fn set_channel(channel: Channel, confirm: Option<GetCSO>) -> std::result::Result<(Channel, Option<GetCSO>), (anyhow::Error, Option<GetCSO>)> {
        let confirm = if let Some(confirm) = confirm {
            if let Err(err) = channel
                // lapin::options::ConfirmSelectOptions { nowait: false }
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

    async fn connect() -> Result<Channel> {
        Comms::create_channel().await
    }

    /// Take the current state and replace it with the invalid state
    fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Invalid)
    }

    fn queue(&mut self, item: T) -> Result<()> {
        match self {
            PublishWrapper::Invalid => anyhow::bail!("Invalid state"),
            PublishWrapper::Idle { queue, .. } => {
                queue.push_back(item);
            }
            PublishWrapper::Sending { queue, .. } => {
                queue.push_back(item);
            }
        }
        Ok(())
    }

    async fn send(item: T,
                  mut channel: Channel,
                  exchange: E,
                  routing_key: K,
                  mut confirm: Option<GetCSO>,
                  publish_options: Option<GetBPO>,
                  publish_properties: Option<GetBP>,
    ) -> Data<E,K> {
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
                            return (channel, exchange, routing_key, confirm, publish_options, publish_properties);
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
                            return (channel, exchange, routing_key, confirm, publish_options, publish_properties);
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

impl<T, E, K> Sink<T> for PublishWrapper<T, E, K>
    where T: Unpin + Send + Serialise + 'static,
          E: AsRef<str> + Send + Unpin + 'static,
          K: AsRef<str> + Send + Unpin + 'static,
{
    type Error = Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let this = Pin::into_inner(self);
        loop {
            match this.take() {
                Self::Sending { mut send, queue } => {
                    let action = send.as_mut();
                    match Future::poll(action, cx) {
                        Poll::Pending => {
                            *this = Self::Sending { send, queue };
                            return Poll::Pending;
                        }
                        Poll::Ready((channel, exchange, routing_key, confirm, publish_options, publish_properties)) => {
                            // the data was send
                            *this = Self::Idle { channel, exchange, routing_key, queue, confirm, publish_options, publish_properties};
                        }
                    }
                }
                Self::Idle { channel, exchange, routing_key, confirm, publish_options, publish_properties, mut queue } => {
                    if let Some(item) = queue.pop_front() {
                       *this = Self::Sending {
                           send: Box::pin(Self::send(item, channel, exchange, routing_key, confirm, publish_options, publish_properties)),
                           queue,
                       };
                    } else {
                        *this = Self::Idle { channel, exchange, routing_key, queue, confirm, publish_options, publish_properties};
                        return Poll::Ready(Ok(()));
                    }
                }
                Self::Invalid => unreachable!(),
            }
            // if let Some(item) = this.queue.pop_front() {
            //     if let Some((channel, exchange, routing_key)) = this.idle.take() {
            //         let confirm = this.confirm;
            //         let action = async move {
            //             let mut channel = channel;
            //             loop {
            //                 // let data = bincode::serialize(&item).expect("Serialise failed");
            //                 let data = Serialise::serialise(&item);
            //                 match channel
            //                     .basic_publish(
            //                         exchange.str(),
            //                         routing_key.str(),
            //                         lapin::options::BasicPublishOptions::default(),
            //                         data,
            //                         BasicProperties::default(),
            //                     )
            //                     .await
            //                 {
            //                     Ok(result) => {
            //                         use lapin::publisher_confirm::Confirmation;
            //                         match (result.await, confirm) {
            //                             (Ok(Confirmation::Ack(None)), true) => {
            //                                 log::trace!(
            //                                     "Publish on channel {} confirmed",
            //                                     channel.id()
            //                                 );
            //                                 return (channel, exchange, routing_key);
            //                             }
            //                             (Ok(_), true) => {
            //                                 log::error!("Failed to confirm message publish on channel {}, repeating the send", channel.id());
            //                                 continue;
            //                             }
            //                             (Err(err), _) => {
            //                                 log::error!("Failed to publish message on channel {}, repeating the send: {}", channel.id(), err);
            //                             }
            //                             (_, false) => {
            //                                 log::trace!("Published");
            //                                 return (channel, exchange, routing_key);
            //                             }
            //                         }
            //                     }
            //                     Err(err) => {
            //                         log::error!(
            //                             "Failed to send message to {} of key {}: {}",
            //                             exchange.str(),
            //                             routing_key.str(),
            //                             err
            //                         );
            //                     }
            //                 }
            //                 loop {
            //                     match Self::set_channel(Self::connect().await, confirm).await {
            //                         Ok(value) => {
            //                             channel = value;
            //                             break;
            //                         }
            //                         Err(err) => {
            //                             log::error!(
            //                                 "Failed to set confirm select on a channel: {}",
            //                                 err
            //                             );
            //                         }
            //                     };
            //                 }
            //             }
            //         };
            //
            //         this.exec = Some(Box::pin(action));
            //     }
            // } else {
            //     return Poll::Ready(Ok(()));
            // }
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
