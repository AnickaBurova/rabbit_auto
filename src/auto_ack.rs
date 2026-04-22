//! RAII ack/nack guard ([`AutoAck`]) and the [`auto_ack`] stream adapter.

use anyhow::Result;
use futures::{Stream, StreamExt};
use lapin::acker::Acker;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions, BasicRejectOptions};

enum Action {
    Ack(Option<BasicAckOptions>),
    Nack(Option<BasicNackOptions>),
    Reject(Option<BasicRejectOptions>),
}

/// RAII guard that acks (or nacks/rejects) a delivery when dropped.
///
/// Obtained from the stream returned by
/// [`StreamBuilder::create_auto_ack`](crate::stream_builder::StreamBuilder::create_auto_ack).
///
/// The default action is **ack**.  Call [`change_to_nack`](AutoAck::change_to_nack) or
/// [`change_to_reject`](AutoAck::change_to_reject) before the guard is dropped to
/// override it.  Call [`execute`](AutoAck::execute) for an explicit async send
/// (the drop will then be a no-op).
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::stream_builder::{StreamBuilder, Deserialise};
/// use futures::StreamExt;
/// use anyhow::Result;
///
/// struct Order { id: u64 }
/// impl Deserialise for Order {
///     fn deserialise(data: Vec<u8>) -> Result<Self> {
///         // parse your message here
///         Ok(Order { id: 0 })
///     }
/// }
///
/// # async fn example() -> Result<()> {
/// let mut stream = StreamBuilder {
///     exchange: "orders",
///     routing_key: "order.placed",
///     tag: "order-processor",
///     ..Default::default()
/// }
/// .create_auto_ack::<Order>()
/// .await;
///
/// while let Some((mut ack, result)) = stream.next().await {
///     match result {
///         Ok(order) => {
///             // process order …
///             // ack is sent automatically on drop
///         }
///         Err(_) => {
///             ack.change_to_nack(None); // will nack on drop
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct AutoAck {
    acker: Option<Acker>,
    action: Action,
}

impl AutoAck {
    /// Creates a guard that acks on drop.
    pub fn new(acker: Acker) -> Self {
        Self {
            acker: Some(acker),
            action: Action::Ack(None),
        }
    }

    /// Creates a guard that acks on drop with explicit [`BasicAckOptions`].
    pub fn new_ack(acker: Acker, options: BasicAckOptions) -> Self {
        Self {
            acker: Some(acker),
            action: Action::Ack(Some(options)),
        }
    }

    /// Creates a guard that nacks on drop with explicit [`BasicNackOptions`].
    pub fn new_nack(acker: Acker, options: BasicNackOptions) -> Self {
        Self {
            acker: Some(acker),
            action: Action::Nack(Some(options)),
        }
    }

    /// Creates a guard that rejects on drop with explicit [`BasicRejectOptions`].
    pub fn new_reject(acker: Acker, options: BasicRejectOptions) -> Self {
        Self {
            acker: Some(acker),
            action: Action::Reject(Some(options)),
        }
    }

    /// Switch the pending action from ack to **nack**.
    ///
    /// Has no effect if [`execute`](AutoAck::execute) or [`release`](AutoAck::release)
    /// has already been called.
    pub fn change_to_nack(&mut self, options: Option<BasicNackOptions>) {
        self.action = Action::Nack(options);
    }

    /// Switch the pending action to **reject**.
    pub fn change_to_reject(&mut self, options: Option<BasicRejectOptions>) {
        self.action = Action::Reject(options);
    }

    /// Take the [`Acker`] out of this guard without sending any AMQP frame.
    ///
    /// After calling `release` the drop is a no-op.  Use this when you want to
    /// manage acknowledgement yourself (e.g. pass the `Acker` somewhere else).
    pub fn release(&mut self) -> Option<Acker> {
        self.acker.take()
    }

    /// Send the ack/nack/reject now, asynchronously.
    ///
    /// Subsequent drops are no-ops.  Prefer this over relying on drop when you
    /// need the error returned or need to ensure the frame is sent before
    /// proceeding.
    pub async fn execute(&mut self) -> Result<()> {
        if let Some(acker) = self.acker.take() {
            match self.action {
                Action::Ack(ref mut options) => Self::do_ack(acker, options.take()).await,
                Action::Nack(ref mut options) => Self::do_nack(acker, options.take()).await,
                Action::Reject(ref mut option) => Self::do_reject(acker, option.take()).await,
            }
        } else {
            Ok(())
        }
    }

    /// Ack the delivery on the channel
    async fn do_ack(acker: Acker, options: Option<BasicAckOptions>) -> Result<()> {
        acker
            .ack(options.unwrap_or_else(|| BasicAckOptions::default()))
            .await?;
        Ok(())
    }
    /// Nack the delivery on the channel
    async fn do_nack(acker: Acker, options: Option<BasicNackOptions>) -> Result<()> {
        acker
            .nack(options.unwrap_or_else(|| BasicNackOptions::default()))
            .await?;
        Ok(())
    }

    async fn do_reject(acker: Acker, options: Option<BasicRejectOptions>) -> Result<()> {
        acker
            .reject(options.unwrap_or_else(|| BasicRejectOptions::default()))
            .await?;
        Ok(())
    }
}

impl Drop for AutoAck {
    fn drop(&mut self) {
        if let Some(acker) = self.acker.take() {
            match self.action {
                Action::Ack(ref mut options) => {
                    tokio::spawn(Self::do_ack(acker, options.take()));
                }
                Action::Nack(ref mut options) => {
                    tokio::spawn(Self::do_nack(acker, options.take()));
                }
                Action::Reject(ref mut options) => {
                    tokio::spawn(Self::do_reject(acker, options.take()));
                }
            }
        }
    }
}

/// Wraps a raw [`Delivery`] stream into an `(AutoAck, payload)` stream.
///
/// Each delivery is paired with an [`AutoAck`] guard.  The guard acks on drop
/// by default; call [`AutoAck::change_to_nack`] or [`AutoAck::execute`] to
/// override.
///
/// This is called internally by
/// [`StreamBuilder::create_auto_ack`](crate::stream_builder::StreamBuilder::create_auto_ack).
/// You only need to call it directly when working with a raw
/// [`create_plain`](crate::stream_builder::StreamBuilder::create_plain) stream.
pub fn auto_ack<S: StreamExt + Stream<Item = Delivery>>(
    stream: S,
) -> impl Stream<Item = (AutoAck, Vec<u8>)> {
    stream.map(|delivery| (AutoAck::new(delivery.acker), delivery.data))
}
