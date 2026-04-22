use lapin::protocol::basic::AMQPProperties;
use crate::publisher::GetBP;

/// Builder for AMQP message properties.
///
/// Provides a friendlier API over the raw [`AMQPProperties`] type.  Once built,
/// convert it into a [`GetBP`] callback (via `Into`) and pass it to
/// [`PublishWrapper::with_static_key`](crate::publisher::PublishWrapper::with_static_key)
/// or [`PublishWrapper::with_dynamic_key`](crate::publisher::PublishWrapper::with_dynamic_key).
///
/// The properties are constructed once and cloned per publish, so any parsing
/// (e.g. `humantime`) happens only at builder time, not on the hot path.
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::publish_properties::PublishProperties;
/// use rabbit_auto::publisher::PublishWrapper;
///
/// # async fn example() {
/// // Set a 10-second TTL on every published message
/// let props: rabbit_auto::publisher::GetBP = PublishProperties::new()
///     .with_expiration_ch(chrono::Duration::seconds(10))
///     .into();
///
/// let publisher = PublishWrapper::<Vec<u8>, _>::with_static_key(
///     "my-exchange",
///     "my.key",
///     false,
///     None,
///     None,
///     Some(props),
/// ).await;
/// # }
/// ```
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
    /// Creates a `PublishProperties` with all fields unset (lapin defaults).
    pub fn new() -> PublishProperties {
        PublishProperties {
            properties: AMQPProperties::default(),
        }
    }

    /// Returns the underlying [`AMQPProperties`].
    pub fn properties(&self) -> &AMQPProperties {
        &self.properties
    }

    /// Sets the per-message TTL from a human-readable duration string.
    ///
    /// Requires the **`humantime`** feature.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "humantime")]
    /// # {
    /// use rabbit_auto::publish_properties::PublishProperties;
    /// let props = PublishProperties::new()
    ///     .with_expiration_hd("30s")
    ///     .unwrap();
    /// # }
    /// ```
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

    /// Sets the per-message TTL from a [`chrono::Duration`].
    ///
    /// Requires the **`chrono`** feature (on by default).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "chrono")]
    /// # {
    /// use rabbit_auto::publish_properties::PublishProperties;
    /// let props = PublishProperties::new()
    ///     .with_expiration_ch(chrono::Duration::seconds(30));
    /// # }
    /// ```
    #[cfg(feature = "chrono")]
    pub fn with_expiration_ch(self, duration: chrono_library::Duration) -> PublishProperties {
        let value = format!("{}", duration.num_milliseconds());
        let properties = self.properties.with_expiration(value.into());
        PublishProperties {
            properties
        }
    }
}


