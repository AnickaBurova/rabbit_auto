use lapin::protocol::basic::AMQPProperties;
use crate::publisher::GetBP;

#[derive(Clone, Debug, Default)]
pub struct PublishProperties {
    properties: AMQPProperties,
}

impl Into<GetBP> for PublishProperties {
    fn into(self) -> GetBP {
        let properties = self.properties;

        Box::pin(move || properties.clone())
    }
}

impl Into<AMQPProperties> for PublishProperties {
    fn into(self) -> AMQPProperties {
        self.properties
    }
}

impl PublishProperties {

    pub fn new() -> PublishProperties {
        PublishProperties {
            properties: AMQPProperties::default(),
        }
    }

    pub fn properties(&self) -> &AMQPProperties {
        &self.properties
    }

    #[cfg(feature = "humantime")]
    pub fn with_expiration_hd(self, duration: &str) -> anyhow::Result<PublishProperties> {
        use humantime_library::Duration;
        let value = duration.parse::<Duration>()?;
        let value = format!("{}", value.as_millis());
        let properties = self.properties.with_expiration(value.into());

        Ok(PublishProperties {
            properties
        })
    }

    #[cfg(feature = "chrono")]
    pub fn with_expiration_ch(self, duration: chrono_library::Duration) -> PublishProperties {
        let value = format!("{}", duration.num_milliseconds());
        let properties = self.properties.with_expiration(value.into());
        PublishProperties {
            properties
        }
    }
}


