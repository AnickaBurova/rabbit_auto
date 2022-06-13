//! Helper to auto ack deliveries



use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions, BasicRejectOptions};
use futures::{StreamExt, Stream};
use anyhow::Result;
use lapin::acker::Acker;

enum Action {
    Ack(Option<BasicAckOptions>),
    Nack(Option<BasicNackOptions>),
    Reject(Option<BasicRejectOptions>),
}
/// Automatically ack delivery when this struct drops.
/// There is an option to automatically nack the delivery.
pub struct AutoAck {
    acker: Option<Acker>,
    action: Action,
}

impl AutoAck {
    /// Creates a new auto ack
    /// # Arguments:
    /// * channel - the channel where this delivery was received on
    /// * delivery - delivery which was received. This will take its id of the delivery.
    pub fn new(acker: Acker) -> Self {
        Self {
            acker: Some(acker),
            action: Action::Ack(None),
        }
    }

    /// Creates a new auto ack with BasicAckOptions
    pub fn new_ack(acker: Acker, options: BasicAckOptions) -> Self {
        Self {
            acker: Some(acker),
            action: Action::Ack(Some(options)),
        }
    }

    pub fn new_nack(acker: Acker, options: BasicNackOptions) -> Self {
        Self {
            acker: Some(acker),
            action: Action::Nack(Some(options)),
        }
    }

    pub fn new_reject(acker: Acker, options: BasicRejectOptions) -> Self {
        Self {
            acker: Some(acker),
            action: Action::Reject(Some(options)),
        }
    }

    /// Change the auto ack in to auto nack
    pub fn change_to_nack(&mut self, options: Option<BasicNackOptions>) {
        self.action = Action::Nack(options);
    }

    pub fn change_to_reject(&mut self, options: Option<BasicRejectOptions>) {
        self.action = Action::Reject(options);
    }

    /// Release the channel and the tag from this
    pub fn release(&mut self) -> Option<Acker> {
        self.acker.take()
    }

    /// Perform the ack or nack on the channel, if this has not been already done or released.
    pub async fn execute(&mut self) -> Result<()> {
        if let Some(acker) = self.acker.take() {
            match self.action {
                Action::Ack(ref mut options) => {
                    Self::do_ack(acker, options.take()).await
                }
                Action::Nack(ref mut options) => {
                    Self::do_nack(acker, options.take()).await
                }
                Action::Reject(ref mut option) => {
                    Self::do_reject(acker, option.take()).await
                }
            }
        } else {
            Ok(())
        }
    }




    /// Ack the delivery on the channel
    async fn do_ack(acker: Acker, options: Option<BasicAckOptions>) -> Result<()> {
        acker.ack( options.unwrap_or_else(|| BasicAckOptions::default())).await?;
        Ok(())
    }
    /// Nack the delivery on the channel
    async fn do_nack(acker: Acker, options: Option<BasicNackOptions>) -> Result<()> {
        acker.nack( options.unwrap_or_else(|| BasicNackOptions::default())).await?;
        Ok(())
    }

    async fn do_reject(acker: Acker, options: Option<BasicRejectOptions>) -> Result<()> {
        acker.reject(options.unwrap_or_else(|| BasicRejectOptions::default())).await?;
        Ok(())
    }
}

impl Drop for AutoAck {
    fn drop(&mut self) {
        if let Some(acker) = self.acker.take() {
            match self.action {
                Action::Ack(ref mut options) => {
                    #[cfg(feature = "tokio_runtime")]
                    tokio::spawn(Self::do_ack(acker, options.take()));
                    #[cfg(feature = "async_std_runtime")]
                    async_std::task::spawn(Self::do_ack(acker, options.take()));
                }
                Action::Nack(ref mut options) => {
                    #[cfg(feature = "tokio_runtime")]
                    tokio::spawn(Self::do_nack(acker, options.take()));
                    #[cfg(feature = "async_std_runtime")]
                    async_std::task::spawn(Self::do_nack(acker, options.take()));
                }
                Action::Reject(ref mut options) => {
                    #[cfg(feature = "tokio_runtime")]
                    tokio::spawn(Self::do_reject(acker, options.take()));
                    #[cfg(feature = "async_std_runtime")]
                    async_std::task::spawn(Self::do_reject(acker, options.take()));
                }
            }
        }
    }
}

/// Map consumer stream into automatically ack stream. The AutoAck object can still be used to release the ack,
/// and manually ack it or not ack it.
pub fn auto_ack<S: StreamExt + Stream<Item = Delivery>>(stream: S) -> impl Stream<Item = (AutoAck, Vec<u8>)> {
    stream.map(|delivery| (AutoAck::new(delivery.acker), delivery.data))
}