// #[cfg(feature = "async_std_runtime")]
// use async_std::sync::RwLock;
use crate::auto_ack::{auto_ack, AutoAck};
use crate::comms::RabbitDispatcher;
use crate::consumer::ConsumerWrapper;
#[cfg(all(feature = "tokio_stream", feature = "chrono"))]
use crate::multi_auto_ack::AutoAckMulti;
use anyhow::Result;
use futures::StreamExt;
use lapin::acker::Acker;
use lapin::message::Delivery;
use lapin::options::QueueDeleteOptions;
use lapin::{Channel, Consumer};

/// Builder for a consumer where the queue name differs from the routing key.
///
/// Identical to [`StreamBuilder`] except it adds a `queue_name` field.  When
/// `queue_name` is `None` the routing key is used as the queue name, which is
/// the same behaviour as `StreamBuilder`.
///
/// Prefer [`StreamBuilder`] for the common case.  Use `StreamBuilderWithName`
/// when you need to bind multiple routing keys to the same queue, or when the
/// queue has a canonical name that is different from the routing key.
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::stream_builder::{StreamBuilderWithName, Deserialise};
/// use futures::StreamExt;
/// use anyhow::Result;
///
/// struct Msg(Vec<u8>);
/// impl Deserialise for Msg {
///     fn deserialise(data: Vec<u8>) -> Result<Self> { Ok(Msg(data)) }
/// }
///
/// # async fn example() {
/// let mut stream = StreamBuilderWithName {
///     queue_name: Some("my-shared-queue"),
///     exchange: "events",
///     routing_key: "user.#",
///     tag: "user-consumer",
///     ..Default::default()
/// }
/// .create_auto_ack::<Msg>()
/// .await;
/// # }
/// ```
#[derive(Default)]
pub struct StreamBuilderWithName<Q, T, K, E> {
    /// Queue name.  Defaults to `routing_key` when `None`.
    pub queue_name: Option<Q>,
    /// Consumer tag (visible in the RabbitMQ management UI).
    pub tag: T,
    /// Exchange to bind the queue to.
    pub exchange: E,
    /// Routing key (also used as queue name when `queue_name` is `None`).
    pub routing_key: K,
    /// If `true`, an incompatible existing queue is deleted and recreated.
    /// If `false` (default), declaration failure is propagated as an error.
    pub incompatible_delete: bool,
    /// QoS prefetch: `(count, options)`.  `None` means no prefetch limit.
    pub qos: Option<(u16, Option<lapin::options::BasicQosOptions>)>,
    /// Options forwarded to `queue_declare`.  `None` uses lapin defaults.
    pub declare: Option<lapin::options::QueueDeclareOptions>,
    /// Extra AMQP table arguments for `queue_declare` (e.g. `x-message-ttl`).
    /// See [`queue_options::QueueDeclare`] for a helper.
    pub declare_fields: Option<lapin::types::FieldTable>,
    /// Options forwarded to `queue_bind`.  `None` uses lapin defaults.
    pub binding: Option<lapin::options::QueueBindOptions>,
    /// Extra AMQP table arguments for `queue_bind`.
    pub binding_fields: Option<lapin::types::FieldTable>,
    /// Options forwarded to `basic_consume`.  `None` uses lapin defaults.
    pub consume: Option<lapin::options::BasicConsumeOptions>,
    /// Extra AMQP table arguments for `basic_consume`.
    pub consume_fields: Option<lapin::types::FieldTable>,
}

#[async_trait::async_trait]
impl<Q, T, K, E> RabbitDispatcher for StreamBuilderWithName<Q, T, K, E>
where
    T: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
    K: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
    E: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
    Q: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
{
    type Object = Consumer;

    async fn dispatcher(&self, channel: &Channel) -> Result<Self::Object> {
        let consumer = {
            if let Some((qos, options)) = self.qos {
                channel
                    .basic_qos(
                        qos,
                        options.unwrap_or_else(|| lapin::options::BasicQosOptions::default()),
                    )
                    .await?;
            }
            let queue_name = if let Some(queue_name) = self.queue_name.as_ref() {
                queue_name.as_ref()
            } else {
                self.routing_key.as_ref()
            };
            if let Err(err) = channel
                .queue_declare(
                    queue_name,
                    self.declare
                        .unwrap_or_else(|| lapin::options::QueueDeclareOptions::default()),
                    self.declare_fields
                        .clone()
                        .unwrap_or_else(|| lapin::types::FieldTable::default()),
                )
                .await
            {
                tracing::warn!("Deleting incompatible queue: {}", err);
                if self.incompatible_delete {
                    let msgs = channel
                        .queue_delete(
                            queue_name,
                            QueueDeleteOptions {
                                if_unused: false,
                                if_empty: false,
                                nowait: true,
                            },
                        )
                        .await?;
                    tracing::warn!("Incompatible queue deleted");
                    if msgs > 0 {
                        tracing::warn!("Deleting the previous queue purged {} messages", msgs);
                    }
                    Err(err)?;
                } else {
                    Err(err)?;
                }
            }
            tracing::trace!("Queue declared, binding it");

            channel
                .queue_bind(
                    queue_name,
                    self.exchange.as_ref(),
                    self.routing_key.as_ref(),
                    self.binding
                        .unwrap_or_else(|| lapin::options::QueueBindOptions::default()),
                    self.binding_fields
                        .clone()
                        .unwrap_or_else(|| lapin::types::FieldTable::default()),
                )
                .await?;
            channel
                .basic_consume(
                    queue_name,
                    self.tag.as_ref(),
                    self.consume
                        .unwrap_or_else(|| lapin::options::BasicConsumeOptions::default()),
                    self.consume_fields
                        .clone()
                        .unwrap_or_else(|| lapin::types::FieldTable::default()),
                )
                .await?
        };
        Ok(consumer)
    }
}

/// Converts the raw AMQP payload bytes into a typed message.
///
/// Implement this for your message type and pass it as the `I` type parameter
/// to [`StreamBuilder::create`], [`StreamBuilder::create_auto_ack`], or
/// [`StreamBuilder::create_multi_auto_ack`].
///
/// # Example
///
/// ```rust
/// use rabbit_auto::stream_builder::Deserialise;
/// use anyhow::Result;
///
/// struct Order { id: u64, item: String }
///
/// impl Deserialise for Order {
///     fn deserialise(data: Vec<u8>) -> Result<Self> {
///         // replace with your actual deserialiser, e.g. serde_json::from_slice
///         let s = String::from_utf8(data)?;
///         Ok(Order { id: 0, item: s })
///     }
/// }
/// ```
pub trait Deserialise: Sized {
    /// Parse `data` into `Self`, returning an error if the payload is invalid.
    fn deserialise(data: Vec<u8>) -> Result<Self>;
}

pub mod queue_options;

impl<Q, T, K, E> StreamBuilderWithName<Q, T, K, E>
where
    T: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
    K: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
    E: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
    Q: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
{
    /// Creates a raw delivery stream.
    ///
    /// Each item is a lapin [`Delivery`].  You are responsible for calling
    /// `ack`/`nack` via `delivery.acker`.  Prefer [`create_auto_ack`](Self::create_auto_ack)
    /// unless you need full control.
    pub async fn create_plain(self) -> impl StreamExt<Item = Delivery> + Unpin + Send {
        ConsumerWrapper::new(Box::new(self)).await
    }

    /// Creates a stream of `(Acker, Result<I>)` pairs.
    ///
    /// The [`Acker`] gives you full manual control over ack/nack.  Use
    /// [`create_auto_ack`](Self::create_auto_ack) if you just want RAII
    /// acknowledgement.
    pub async fn create<I: Deserialise>(
        self,
    ) -> impl StreamExt<Item = (Acker, Result<I>)> + Unpin + Send {
        let consumer = self.create_plain().await;
        consumer.map(|delivery| (delivery.acker, I::deserialise(delivery.data)))
    }

    /// Creates a stream of `(AutoAck, Result<I>)` pairs.
    ///
    /// Each delivery is paired with an [`AutoAck`] guard that acks on drop.
    /// Call [`AutoAck::change_to_nack`] before dropping to nack instead, or
    /// [`AutoAck::execute`] to send the frame immediately.
    pub async fn create_auto_ack<I: Deserialise>(
        self,
    ) -> impl StreamExt<Item = (AutoAck, Result<I>)> + Unpin + Send {
        let consumer = self.create_plain().await;
        auto_ack(consumer).map(|(ack, delivery)| (ack, I::deserialise(delivery)))
    }

    /// Creates a batched-ack stream of `(AutoAckMulti, Result<I>)` pairs.
    ///
    /// Requires features **`tokio_stream`** and **`chrono`** (both on by default).
    ///
    /// Instead of sending one AMQP frame per message, decisions are accumulated
    /// and flushed every `period` using `basic_ack(multiple=true)` /
    /// `basic_nack(multiple=true)`.  This dramatically reduces broker round-trips
    /// for high-throughput consumers.
    ///
    /// # Arguments
    ///
    /// * `period` — how often to flush pending decisions to the broker.
    /// * `bounded_buffer_size` — capacity of the internal delivery channel.
    ///   When the downstream consumer is slower than the broker, messages are
    ///   buffered here before backpressure kicks in.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rabbit_auto::stream_builder::{StreamBuilder, Deserialise};
    /// use futures::StreamExt;
    /// use anyhow::Result;
    ///
    /// struct Msg(Vec<u8>);
    /// impl Deserialise for Msg {
    ///     fn deserialise(data: Vec<u8>) -> Result<Self> { Ok(Msg(data)) }
    /// }
    ///
    /// # async fn example() {
    /// let mut stream = StreamBuilder {
    ///     exchange: "events",
    ///     routing_key: "my.key",
    ///     tag: "batch-consumer",
    ///     ..Default::default()
    /// }
    /// .create_multi_auto_ack::<Msg>(
    ///     chrono::Duration::milliseconds(500),
    ///     256,
    /// )
    /// .await;
    ///
    /// while let Some((mut ack, result)) = stream.next().await {
    ///     if result.is_err() {
    ///         ack.change_to_nack();
    ///     }
    ///     // decision is sent on drop; broker receives batched frames every 500 ms
    /// }
    /// # }
    /// ```
    #[cfg(all(feature = "tokio_stream", feature = "chrono"))]
    pub async fn create_multi_auto_ack<I: Deserialise + Send + 'static>(
        self,
        period: chrono_library::Duration,
        bounded_buffer_size: usize,
    ) -> impl StreamExt<Item = (AutoAckMulti, Result<I>)> + Unpin + Send {
        let mut consumer = self.create_plain().await;
        let (sx, rx) = tokio::sync::mpsc::channel(bounded_buffer_size);
        tokio::spawn(async move {
            use std::collections::VecDeque;

            let (sc_tick, mut rx_tick) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                let period = period.to_std().unwrap();
                loop {
                    tokio::time::sleep(period).await;
                    sc_tick.send(()).await.unwrap();
                }
            });
            let (mut commits, mut action_receiver) = crate::multi_auto_ack::Commits::new();
            let mut backlog = VecDeque::with_capacity(bounded_buffer_size);
            loop {
                let mut commiting = false;
                if backlog.is_empty() {
                    tokio::select! {
                        Some(delivery) = consumer.next() => {
                            let (ack, data) = commits.add_message(delivery);
                            backlog.push_back((ack, I::deserialise(data)));
                        }
                        Some(action) = action_receiver.recv() => {
                            commits.commit_tag(action);
                        }
                        _ = rx_tick.recv() => {
                            commiting = true;
                        }
                    }

                } else {
                    tokio::select! {
                        Some(action) = action_receiver.recv() => {
                            commits.commit_tag(action);
                        }
                        _ = rx_tick.recv() => {
                            commiting = true;
                        }
                    }
                }

                while let Some(msg) = backlog.pop_front() {
                    if let Err(err) = sx.try_send(msg) {
                        match err {
                            tokio::sync::mpsc::error::TrySendError::Full(msg) => {
                                backlog.push_front(msg);
                                break;
                            }
                            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                return;
                            }
                        }
                    }
                }
                if commiting {
                    tracing::warn!("Performing auto-acking");
                    // Drain all decisions that arrived since the last tick
                    // before flushing, so the batch is as complete as possible.
                    while let Ok(action) = action_receiver.try_recv() {
                        commits.commit_tag(action);
                    }
                    commits.perform_auto_acking().await.unwrap();
                }
            }
        });
        tokio_stream_library::wrappers::ReceiverStream::new(rx)
    }
}

/// Builder for an auto-reconnecting consumer stream.
///
/// Set the required fields (`exchange`, `routing_key`, `tag`) and call one of
/// the `create_*` methods to obtain a stream.  All other fields are optional
/// and default to the lapin defaults.
///
/// The queue name is always equal to the routing key.  If you need an
/// independent queue name use [`StreamBuilderWithName`] instead.
///
/// # Example — minimal setup
///
/// ```rust,no_run
/// use rabbit_auto::stream_builder::{StreamBuilder, Deserialise};
/// use futures::StreamExt;
/// use anyhow::Result;
///
/// struct Msg(Vec<u8>);
/// impl Deserialise for Msg {
///     fn deserialise(data: Vec<u8>) -> Result<Self> { Ok(Msg(data)) }
/// }
///
/// # async fn example() {
/// let mut stream = StreamBuilder {
///     exchange: "events",
///     routing_key: "order.placed",
///     tag: "order-service",
///     ..Default::default()
/// }
/// .create_auto_ack::<Msg>()
/// .await;
///
/// while let Some((ack, result)) = stream.next().await {
///     // process result; ack sent on drop
/// }
/// # }
/// ```
///
/// # Example — durable queue with QoS
///
/// ```rust,no_run
/// use rabbit_auto::stream_builder::StreamBuilder;
/// use lapin::options::{BasicQosOptions, QueueDeclareOptions};
///
/// let builder = StreamBuilder {
///     exchange: "events",
///     routing_key: "order.placed",
///     tag: "order-service",
///     declare: Some(QueueDeclareOptions { durable: true, ..Default::default() }),
///     qos: Some((64, Some(BasicQosOptions { global: false }))),
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Default)]
pub struct StreamBuilder<T, K, E> {
    /// Consumer tag (visible in the RabbitMQ management UI).
    pub tag: T,
    /// Exchange to bind the queue to.
    pub exchange: E,
    /// Routing key; also used as the queue name.
    pub routing_key: K,
    /// If `true`, an incompatible existing queue is deleted and recreated.
    /// If `false` (default), declaration failure is propagated as an error.
    pub incompatible_delete: bool,
    /// QoS prefetch: `(count, options)`.  `None` means no prefetch limit.
    pub qos: Option<(u16, Option<lapin::options::BasicQosOptions>)>,
    /// Options forwarded to `queue_declare`.  `None` uses lapin defaults.
    pub declare: Option<lapin::options::QueueDeclareOptions>,
    /// Extra AMQP table arguments for `queue_declare` (e.g. `x-message-ttl`).
    /// See [`queue_options::QueueDeclare`] for a helper.
    pub declare_fields: Option<lapin::types::FieldTable>,
    /// Options forwarded to `queue_bind`.  `None` uses lapin defaults.
    pub binding: Option<lapin::options::QueueBindOptions>,
    /// Extra AMQP table arguments for `queue_bind`.
    pub binding_fields: Option<lapin::types::FieldTable>,
    /// Options forwarded to `basic_consume`.  `None` uses lapin defaults.
    pub consume: Option<lapin::options::BasicConsumeOptions>,
    /// Extra AMQP table arguments for `basic_consume`.
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
where
    T: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
    K: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
    E: AsRef<str> + Send + Unpin + 'static + Clone + Sync,
{
    /// Creates a raw delivery stream.
    ///
    /// See [`StreamBuilderWithName::create_plain`] for details.
    pub async fn create_plain(self) -> impl StreamExt<Item = Delivery> + Unpin + Send {
        self.into_with_name().create_plain().await
    }

    /// Creates a stream of `(Acker, Result<I>)` pairs.
    ///
    /// See [`StreamBuilderWithName::create`] for details.
    pub async fn create<I: Deserialise>(
        self,
    ) -> impl StreamExt<Item = (Acker, Result<I>)> + Unpin + Send {
        let consumer = self.create_plain().await;
        consumer.map(|delivery| (delivery.acker, I::deserialise(delivery.data)))
    }

    /// Creates a stream of `(AutoAck, Result<I>)` pairs.
    ///
    /// See [`StreamBuilderWithName::create_auto_ack`] for details.
    pub async fn create_auto_ack<I: Deserialise>(
        self,
    ) -> impl StreamExt<Item = (AutoAck, Result<I>)> + Unpin + Send {
        let consumer = self.create_plain().await;
        auto_ack(consumer).map(|(ack, delivery)| (ack, I::deserialise(delivery)))
    }

    /// Creates a batched-ack stream of `(AutoAckMulti, Result<I>)` pairs.
    ///
    /// See [`StreamBuilderWithName::create_multi_auto_ack`] for details.
    #[cfg(all(feature = "tokio_stream", feature = "chrono"))]
    pub async fn create_multi_auto_ack<I: Deserialise + Send + 'static>(
        self,
        period: chrono_library::Duration,
        bounded_buffer_size: usize,
    ) -> impl StreamExt<Item = (AutoAckMulti, Result<I>)> + Unpin + Send {
        let mut consumer = self.create_plain().await;
        let (sx, rx) = tokio::sync::mpsc::channel(bounded_buffer_size);
        tokio::spawn(async move {
            use std::collections::VecDeque;

            let (sc_tick, mut rx_tick) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                let period = period.to_std().unwrap();
                loop {
                    tokio::time::sleep(period).await;
                    sc_tick.send(()).await.unwrap();
                }
            });
            let (mut commits, mut action_receiver) = crate::multi_auto_ack::Commits::new();
            let mut backlog = VecDeque::with_capacity(bounded_buffer_size);
            loop {
                let mut commiting = false;
                if backlog.is_empty() {
                    tokio::select! {
                        Some(delivery) = consumer.next() => {
                            let (ack, data) = commits.add_message(delivery);
                            backlog.push_back((ack, I::deserialise(data)));
                        }
                        Some(action) = action_receiver.recv() => {
                            commits.commit_tag(action);
                        }
                        _ = rx_tick.recv() => {
                            commiting = true;
                        }
                    }

                } else {
                    tokio::select! {
                        Some(action) = action_receiver.recv() => {
                            commits.commit_tag(action);
                        }
                        _ = rx_tick.recv() => {
                            commiting = true;
                        }
                    }
                }

                while let Some(msg) = backlog.pop_front() {
                    if let Err(err) = sx.try_send(msg) {
                        match err {
                            tokio::sync::mpsc::error::TrySendError::Full(msg) => {
                                backlog.push_front(msg);
                                break;
                            }
                            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                return;
                            }
                        }
                    }
                }
                if commiting {
                    tracing::warn!("Performing auto-acking");
                    // Drain all decisions that arrived since the last tick
                    // before flushing, so the batch is as complete as possible.
                    while let Ok(action) = action_receiver.try_recv() {
                        commits.commit_tag(action);
                    }
                    commits.perform_auto_acking().await.unwrap();
                }
            }
        });
        tokio_stream_library::wrappers::ReceiverStream::new(rx)
    }
}
