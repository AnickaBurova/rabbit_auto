use futures::{StreamExt};
use crate::auto_ack::{AutoAck, auto_ack};
use lapin::Channel;
use crate::consumer::ConsumerWrapper;
use anyhow::Result;

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
    pub async fn create_auto_ack<I: Deserialise>(self) -> Result<impl StreamExt<Item = (AutoAck, Result<I>)> + Unpin> {
        let consumer = ConsumerWrapper::new(Box::pin(move |channel: Channel| {
            // log::trace!("Declaring rabbit '{}' queue", stringify!($item));
            let this = self.clone();
            Box::pin(async move {
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
                let consumer = channel
                    .basic_consume(
                        this.routing_key.as_ref(),
                        this.tag.as_ref(),
                        this.consume.unwrap_or_else(|| lapin::options::BasicConsumeOptions::default()),
                        this.consume_fields.unwrap_or_else(|| lapin::types::FieldTable::default()),
                )
                .await?;
                Ok((channel, consumer))
            })
        }))
            .await?;
        Ok(auto_ack(consumer).map(|(ack, delivery)| (ack, I::deserialise(delivery.data))))
    }
}


#[macro_export]
macro_rules! stream {
    ($function: ident <$item: ident> $($auto_ack: ident)*{
         tag = $tag: expr,
         exchange =  $exchange: expr,
         $(qos = $qos: expr,)*
         declare = [$($declare: ident = $decl_value: expr;)*]
         fields = [$($fields: literal = $fields_value: expr;)*]
         }
    ) => {
            pub async fn $function<K: AsRef<str>>(routing_key: K) -> stream!{result $($auto_ack)*} {
                let consumer = rabbit_auto::consumer::ConsumerWrapper::new(Box::pin(move |channel: Channel| {
                    log::trace!("Declaring rabbit '{}' queue", stringify!($item));
                    Box::pin(async move {
                        $(
                            channel.basic_qos($qos, lapin::options::BasicQosOptions::default()).await?;
                        )*
                        let declare = lapin::options::QueueDeclareOptions {
                            $(declare.$declare = $decl_value,)*
                            ..lapin::options::QueueDeclareOptions::default()};
                        stream!{let args $($fields)*} = lapin::types::FieldTable::default();
                        $(
                            args.insert($fields.into(), lapin::types::AMQPValue::LongUInt($fields_value));
                        )*
                        channel.queue_declare(routing_key.as_ref(), declare, args).await?;
                        log::trace!("Queue declared, binding it");
                        channel
                            .queue_bind(
                                routing_key.as_ref(),
                                rabbit::$exchange,
                                routing_key.as_ref(),
                                lapin::options::QueueBindOptions::default(),
                                FieldTable::default(),
                            )
                            .await?;
                        log::trace!(
                            "Creating rabbit '{}' consumer at {}",
                            stringify!($item),
                            channel.id()
                        );
                        let consumer = channel
                            .basic_consume(
                                routing_key.as_ref(),
                                $tag,
                                BasicConsumeOptions::default(),
                                FieldTable::default(),
                            )
                            .await?;
                        Ok((channel, consumer))
                    })
                }))
                .await;
                log::trace!("Got consumer");
                stream!{return $($auto_ack)*}
            }
    };

    (return auto_ack) => {
        rabbit_auto::auto_ack::auto_ack(consumer)
    };

    (return) => {
        consumer
    };

    (result auto_ack) => {
        impl Stream<Item = (rabbit_auto::auto_ack::AutoAck, lapin::message::Delivery)>
    };
    (result ) =>  {
        impl Stream<Item = (lapin::Channel, lapin::message::Delivery)>
    };

    (let $name: ident $($anything: ident)+) => {
        let mut $name
    };
    (let $name: ident ) => {
        let $name
    };

}
