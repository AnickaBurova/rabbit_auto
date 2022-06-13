use std::sync::Arc;
use futures::{StreamExt};
use crate::auto_ack::{AutoAck, auto_ack};
use lapin::Channel;
use crate::consumer::ConsumerWrapper;
use anyhow::Result;
use futures::lock::Mutex;
use lapin::acker::Acker;
use lapin::message::Delivery;

#[derive(Clone, Default)]
pub struct StreamBuilder<T, K, E> {
    pub tag: T,
    pub exchange: E,
    pub routing_key: K,
    pub qos: Option<(u16, Option<lapin::options::BasicQosOptions>)>,
    pub declare: Option<lapin::options::QueueDeclareOptions>,
    pub declare_fields: Option<lapin::types::FieldTable>,
    pub binding: Option<lapin::options::QueueBindOptions>,
    pub binding_fields: Option<lapin::types::FieldTable>,
    pub consume: Option<lapin::options::BasicConsumeOptions>,
    pub consume_fields: Option<lapin::types::FieldTable>,
}

pub trait Deserialise: Sized {
    fn deserialise(data: Vec<u8>) -> Result<Self>;
}

impl<T,K,E> StreamBuilder<T, K, E>
    where T: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          K: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
          E: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
{
    /// Creates a consumer which returns channel and the delivery.
    pub async fn create_plain(self) -> impl StreamExt<Item = Delivery> + Unpin + Send {
        let consumer = ConsumerWrapper::new(Box::pin(move |channel: Arc<Mutex<Channel>>| {
            // log::trace!("Declaring rabbit '{}' queue", stringify!($item));
            let this = self.clone();
            Box::pin(async move {
                let consumer = {
                    let channel = channel.lock().await;
                    if let Some((qos, options)) = this.qos {
                        channel.basic_qos(qos,
                                          options.unwrap_or_else(|| lapin::options::BasicQosOptions::default())
                        ).await?;
                    }
                    channel.queue_declare(this.routing_key.as_ref(),
                                          this.declare
                                              .unwrap_or_else(|| lapin::options::QueueDeclareOptions::default()),
                                          this.declare_fields.unwrap_or_else(|| lapin::types::FieldTable::default()),
                    ).await?;
                    log::trace!("Queue declared, binding it");
                    channel
                        .queue_bind(
                            this.routing_key.as_ref(),
                            this.exchange.as_ref(),
                            this.routing_key.as_ref(),
                            this.binding.unwrap_or_else(|| lapin::options::QueueBindOptions::default()),
                            this.binding_fields.unwrap_or_else(|| lapin::types::FieldTable::default()),
                        )
                        .await?;
                    // log::trace!(
                    //     "Creating rabbit '{}' consumer at {}",
                    //     stringify!($item),
                    //     channel.id()
                    // );
                    channel
                        .basic_consume(
                            this.routing_key.as_ref(),
                            this.tag.as_ref(),
                            this.consume.unwrap_or_else(|| lapin::options::BasicConsumeOptions::default()),
                            this.consume_fields.unwrap_or_else(|| lapin::types::FieldTable::default()),
                        )
                        .await?
                };
                Ok((channel, consumer))
            })
        }))
            .await;
       consumer
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
