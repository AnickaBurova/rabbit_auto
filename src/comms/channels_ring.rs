use std::sync::{Arc};
use lapin::{Channel, Connection};


pub(crate) struct Channels {
    channels: Vec<Arc<Channel>>,
    current:  usize,
}

impl Channels {
    pub(crate) fn new() -> Self {
        Self {
            channels: Vec::with_capacity(super::MAX_CHANNELS),
            current:  0,
        }
    }

    pub async fn create_channels(&mut self, count: usize, connection: &Connection) -> anyhow::Result<Vec<Arc<Channel>>> {
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            result.push(self.create_channel(connection).await?);
        }
        Ok(result)
    }

    pub(crate) async fn create_channel(&mut self, connection: &Connection) -> anyhow::Result<Arc<Channel>> {
        let index = self.current;
        self.current = (self.current + 1) % super::MAX_CHANNELS;
        if self.channels.len() < index + 1 {
            log::debug!("Creating a new channel: {index}");
            let channel = Arc::new(connection.create_channel().await?);
            self.channels.push(channel.clone());
            Ok(channel)
        } else {
            log::debug!("Returning ring channel {index}");
            Ok(self.channels[index].clone())
        }
    }

    pub(crate) async fn try_close(&mut self) {
        log::trace!("Closing all channels: {}", self.channels.len());
        for channel in self.channels.drain(..) {
            log::trace!("Closing channel[{}]: {:?}", channel.id(), channel.status().state());
            if let Err(err) = channel.close(0, "Restarting the connection").await {
                log::error!("Failed to close an existing channel: {err}");
            }
        }
        self.current = 0;
    }
}

