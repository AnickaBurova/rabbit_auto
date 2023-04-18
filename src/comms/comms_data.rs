use lapin::{Channel, Connection, ConnectionStatus};
use std::sync::Arc;
use futures::lock::Mutex;

pub const MAX_CHANNELS: usize = 8;

pub struct Data {
    /// The actual connection to the rabbit
    connection: Connection,
    /// Precreated channel to use, this will not go over MAX_CHANNELS
    channels: Vec<Arc<Mutex<Channel>>>,
    /// The current index of the channel to return.
    /// Creating channel will go around `channels` in a circle up to MAX_CHANNELS
    current_channel_index: usize,
}

impl Data {
    /// Creates a new rabbit connection
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            channels: Vec::with_capacity(MAX_CHANNELS),
            current_channel_index: 0,
        }
    }

    /// Gets the connection status
    pub fn get_status(&self) -> &ConnectionStatus {
        self.connection.status()
    }

    /// Create a channel, or take one already created from a ring buffer
    pub async fn create_channel(&mut self) -> anyhow::Result<Arc<Mutex<Channel>>> {
        let index = self.current_channel_index;
        self.current_channel_index += 1;
        if self.current_channel_index > MAX_CHANNELS {
            self.current_channel_index = 0;
        }
        if self.channels.len() < index + 1 {
            log::trace!("Creating a new channel: {}", self.channels.len());
            let channel = Arc::new(Mutex::new(self.connection.create_channel().await?));
            self.channels.push(channel.clone());
            Ok(channel)
        } else {
            log::trace!("Returning ring channel {}", index);
            Ok(self.channels[index].clone())
        }
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        for channel in self.channels.drain(..) {
            let channel = channel.lock().await;

            if let Err(err) = channel.close(0, "restarting connection").await {
                log::error!("Failed to close a channel: {}", err);
            }
        }
        self.current_channel_index = 0;
        log::warn!("Closing rabbit connection");
        self.connection.close(0, "Restarting connection").await?;
        Ok(())
    }
}
