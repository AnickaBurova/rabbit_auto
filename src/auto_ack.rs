//! Helper to auto ack deliveries



use lapin::Channel;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use futures::{StreamExt, Stream};
use anyhow::Result;

enum Action {
    Ack(Option<BasicAckOptions>),
    Nack(Option<BasicNackOptions>),
}
/// Automatically ack delivery when this struct drops.
pub struct AutoAck {
    args: Option<(Channel, u64)>,
    action: Action,
}

impl AutoAck {
    /// Creates a new auto ack
    /// # Arguments:
    /// * channel - the channel where this delivery was received on
    /// * delivery - delivery which was received. This will take its id of the delivery.
    pub fn new(channel: Channel, delivery: &Delivery) -> Self {
        Self {
            args: Some((channel, delivery.delivery_tag)),
            action: Action::Ack(None),
        }
    }

    /// Creates a new auto ack with BasicAckOptions
    pub fn new_ack(channel: Channel, delivery: &Delivery, options: BasicAckOptions) -> Self {
        Self {
            args: Some((channel, delivery.delivery_tag)),
            action: Action::Ack(Some(options)),
        }
    }

    pub fn new_nack(channel: Channel, delivery: &Delivery, options: BasicNackOptions) -> Self {
        Self {
            args: Some((channel, delivery.delivery_tag)),
            action: Action::Nack(Some(options)),
        }
    }

    /// Change the auto ack in to auto nack
    pub fn change_to_nack(&mut self, options: Option<BasicNackOptions>) {
        self.action = Action::Nack(options);
    }

    /// Release the channel and the tag from this
    pub fn release(&mut self) -> Option<(Channel, u64)> {
        self.args.take()
    }

    /// Perform the ack or nack on the channel, if this has not been already done or released.
    pub async fn execute(&mut self) -> Result<()> {
        if let Some((channel, tag)) = self.args.take() {
            match self.action {
                Action::Ack(ref mut options) => {
                    Self::do_ack(channel, tag, options.take()).await
                }
                Action::Nack(ref mut options) => {
                    Self::do_nack(channel, tag, options.take()).await
                }
            }
        } else {
            Ok(())
        }
    }




    /// Ack the delivery on the channel
    async fn do_ack(channel: Channel, tag: u64, options: Option<BasicAckOptions>) -> Result<()> {
        channel.basic_ack(tag, options.unwrap_or_else(|| BasicAckOptions::default())).await?;
        Ok(())
    }
    /// Nack the delivery on the channel
    async fn do_nack(channel: Channel, tag: u64, options: Option<BasicNackOptions>) -> Result<()> {
        channel.basic_nack(tag, options.unwrap_or_else(|| BasicNackOptions::default())).await?;
        Ok(())
    }
}

impl Drop for AutoAck {
    fn drop(&mut self) {
        if let Some((channel, tag)) = self.args.take() {
            match self.action {
                Action::Ack(options) => {
                    #[cfg(feature = "tokio_runtime")]
                        tokio::spawn(Self::do_ack(channel, tag, options));
                }
                Action::Nack(options) => {
                    #[cfg(feature = "tokio_runtime")]
                        tokio::spawn(Self::do_nack(channel, tag, options));
                }
            }
        }
    }
}

/// Map consumer stream into automatically ack stream. The AutoAck object can still be used to release the ack,
/// and manually ack it or not ack it.
pub fn auto_ack<S: StreamExt + Stream<Item = (Channel, Delivery)>>(stream: S) -> impl Stream<Item = (AutoAck, Delivery)> {
    stream.map(|(channel, delivery)| (AutoAck::new(channel, &delivery), delivery))
}