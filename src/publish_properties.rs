use lapin::protocol::basic::AMQPProperties;
use crate::publisher::GetBP;

/// Creates a publish properties using helper functions for accepting values which makes more sense
/// than strings in AMQPProperties
/// After creating the PublishProperties, it can be changed into GetBP, which is a callback in PublishWrapper
/// to get the AMQPProperties. The properties are constructed before the callback and cloned when needed.
/// So using for example humantime parse is not going to slow down the process.
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
    /// Set the expiration of the message using human time duration. This feature is optional "humantime".
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
    /// Set the expiration of the message using chrono duration. This feature is optional "chrono".
    pub fn with_expiration_ch(self, duration: chrono_library::Duration) -> PublishProperties {
        let value = format!("{}", duration.num_milliseconds());
        let properties = self.properties.with_expiration(value.into());
        PublishProperties {
            properties
        }
    }
}


