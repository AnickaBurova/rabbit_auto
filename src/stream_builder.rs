// #[cfg(feature = "async_std_runtime")]
// use async_std::sync::RwLock;
use futures::{StreamExt};
use crate::auto_ack::{AutoAck, auto_ack};
use lapin::{Channel, Consumer};
use crate::consumer::ConsumerWrapper;
use anyhow::Result;
use lapin::acker::Acker;
use lapin::message::Delivery;
use lapin::options::QueueDeleteOptions;
use crate::comms::{RabbitDispatcher};

#[derive(Default)]
pub struct StreamBuilderWithName<Q, T, K, E> {
    /// Gives this name to the queue, or the default would be the `routing_key`.
    pub queue_name: Option<Q>,
    /// The tag of the consumer
    pub tag: T,
    /// The exchange where to bind this queue
    pub exchange: E,
    /// The routing key of the consumer
    pub routing_key: K,
    /// When creating the queue, delete the old if it is not compatible
    pub incompatible_delete: bool,
    pub qos: Option<(u16, Option<lapin::options::BasicQosOptions>)>,
    pub declare: Option<lapin::options::QueueDeclareOptions>,
    pub declare_fields: Option<lapin::types::FieldTable>,
    pub binding: Option<lapin::options::QueueBindOptions>,
    pub binding_fields: Option<lapin::types::FieldTable>,
    pub consume: Option<lapin::options::BasicConsumeOptions>,
    pub consume_fields: Option<lapin::types::FieldTable>,
}

#[async_trait::async_trait]
impl<Q, T, K, E> RabbitDispatcher for StreamBuilderWithName<Q, T, K, E>
    where T: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          K: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          E: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          Q: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
{
    type Object = Consumer;

    async fn dispatcher(&self, channel: &Channel) -> Result<Self::Object> {
        let consumer = {
            if let Some((qos, options)) = self.qos {
                channel.basic_qos(qos,
                                  options.unwrap_or_else(|| lapin::options::BasicQosOptions::default())
                ).await?;
            }
            let queue_name = if let Some(queue_name) = self.queue_name.as_ref() {
                queue_name.as_ref()
            } else {
                self.routing_key.as_ref()
            };
            if let Err(err) = channel.queue_declare(queue_name,
                                                    self.declare
                                                        .unwrap_or_else(|| lapin::options::QueueDeclareOptions::default()),
                                                    self.declare_fields.clone().unwrap_or_else(|| lapin::types::FieldTable::default()),
            ).await {
                log::warn!("Deleting incompatible queue: {}", err);
                if self.incompatible_delete {
                    let msgs = channel.queue_delete(queue_name, QueueDeleteOptions {
                        if_unused: false,
                        if_empty: false,
                        nowait: true,
                    }).await?;
                    log::warn!("Incompatible queue deleted");
                    if msgs > 0 {
                        log::warn!("Deleting the previous queue purged {} messages", msgs);
                    }
                    Err(err)?;
                } else {
                    Err(err)?;
                }
            }
            log::trace!("Queue declared, binding it");

            channel
                .queue_bind(
                    queue_name,
                    self.exchange.as_ref(),
                    self.routing_key.as_ref(),
                    self.binding.unwrap_or_else(|| lapin::options::QueueBindOptions::default()),
                    self.binding_fields.clone().unwrap_or_else(|| lapin::types::FieldTable::default()),
                )
                .await?;
            channel
                .basic_consume(
                    queue_name,
                    self.tag.as_ref(),
                    self.consume.unwrap_or_else(|| lapin::options::BasicConsumeOptions::default()),
                    self.consume_fields.clone().unwrap_or_else(|| lapin::types::FieldTable::default()),
                )
                .await?
        };
        Ok(consumer)
    }
}

pub trait Deserialise: Sized {
    fn deserialise(data: Vec<u8>) -> Result<Self>;
}


pub mod queue_options;

impl<Q, T,K,E> StreamBuilderWithName<Q, T, K, E>
    where T: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          K: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          E: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          Q: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
{
    /// Creates a consumer which returns channel and the delivery.
    pub async fn create_plain(self) -> impl StreamExt<Item = Delivery> + Unpin + Send {
        ConsumerWrapper::new(Box::new(self)).await
    }

    /// Creates a consumer which returns an acker and the deserialised item
    pub async fn create<I: Deserialise>(self) -> impl StreamExt<Item = (Acker, Result<I>)> + Unpin + Send {
        let consumer = self.create_plain().await;
        consumer.map(| delivery| (delivery.acker, I::deserialise(delivery.data)))
    }

    /// Creates a consumer which returns autoack and the item
    pub async fn create_auto_ack<I: Deserialise>(self) -> impl StreamExt<Item = (AutoAck, Result<I>)> + Unpin + Send {
        let consumer = self.create_plain().await;
        auto_ack(consumer).map(|(ack, delivery)| (ack, I::deserialise(delivery)))
    }
}


#[derive(Clone, Default)]
pub struct StreamBuilder<T, K, E> {
    /// The tag of the consumer
    pub tag: T,
    /// The exchange where to bind this queue
    pub exchange: E,
    /// The routing key of the consumer
    pub routing_key: K,
    /// When creating the queue, delete the old if it is not compatible
    pub incompatible_delete: bool,
    pub qos: Option<(u16, Option<lapin::options::BasicQosOptions>)>,
    pub declare: Option<lapin::options::QueueDeclareOptions>,
    pub declare_fields: Option<lapin::types::FieldTable>,
    pub binding: Option<lapin::options::QueueBindOptions>,
    pub binding_fields: Option<lapin::types::FieldTable>,
    pub consume: Option<lapin::options::BasicConsumeOptions>,
    pub consume_fields: Option<lapin::types::FieldTable>,
}

impl<T, K, E> StreamBuilder<T, K, E> {
    fn into_with_name(self) -> StreamBuilderWithName<&'static str, T, K, E> {
        StreamBuilderWithName::<&'static str, T, K, E> {
            tag: self.tag,
            exchange: self.exchange,
            routing_key: self.routing_key,
            queue_name: None,
            incompatible_delete: self.incompatible_delete,
            qos: self.qos,
            declare: self.declare,
            declare_fields: self.declare_fields,
            binding: self.binding,
            binding_fields: self.binding_fields,
            consume: self.consume,
            consume_fields: self.consume_fields,
        }
    }
}


impl<T, K, E> StreamBuilder<T, K, E>
    where T: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          K: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          E: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
{
    /// Creates a consumer which returns channel and the delivery.
    pub async fn create_plain(self) -> impl StreamExt<Item = Delivery> + Unpin + Send {
            self.into_with_name().create_plain().await
    }

    /// Creates a consumer which returns an acker and the deserialised item
    pub async fn create<I: Deserialise>(self) -> impl StreamExt<Item = (Acker, Result<I>)> + Unpin + Send {
        let consumer = self.create_plain().await;
        consumer.map(| delivery| (delivery.acker, I::deserialise(delivery.data)))
    }

    /// Creates a consumer which returns autoack and the item
    pub async fn create_auto_ack<I: Deserialise>(self) -> impl StreamExt<Item = (AutoAck, Result<I>)> + Unpin + Send {
        let consumer = self.create_plain().await;
        auto_ack(consumer).map(|(ack, delivery)| (ack, I::deserialise(delivery)))
    }
}
