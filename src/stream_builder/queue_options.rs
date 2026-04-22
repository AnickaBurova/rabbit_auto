use std::time::Duration;
use lapin::types::FieldTable;

/// Builder for AMQP queue-declare extra arguments (`x-*` fields).
///
/// Convert to a [`FieldTable`] (via `Into`) and assign to
/// [`StreamBuilder::declare_fields`](crate::stream_builder::StreamBuilder::declare_fields).
///
/// # Example
///
/// ```rust,no_run
/// use rabbit_auto::stream_builder::{StreamBuilder, queue_options::QueueDeclare};
/// use std::time::Duration;
///
/// let builder = StreamBuilder {
///     exchange: "events",
///     routing_key: "order.placed",
///     tag: "order-service",
///     declare_fields: Some(QueueDeclare::ttl(Duration::from_secs(300)).into()),
///     ..Default::default()
/// };
/// ```
#[derive(Default)]
pub struct QueueDeclare {
    ttl: Option<Duration>,
}

impl Into<FieldTable> for QueueDeclare {
    fn into(self) -> FieldTable {
        let mut fields = FieldTable::default();
        if let Some(ttl) = self.ttl {
            fields.insert("x-message-ttl".into(), (ttl.as_millis() as u32).into());
        }
        fields
    }
}

impl QueueDeclare {
    /// Creates a `QueueDeclare` that sets the `x-message-ttl` queue argument.
    ///
    /// Messages older than `duration` will be dead-lettered or dropped by the broker.
    pub fn ttl(duration: Duration) -> QueueDeclare {
        Self { ttl: Some(duration) }
    }
}
