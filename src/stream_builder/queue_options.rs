use std::time::Duration;
use lapin::types::{ FieldTable};

/// Wrapper for queue declare fields
#[derive(Default)]
pub struct QueueDeclare {
    ttl: Option<Duration>,
}

impl Into<FieldTable> for QueueDeclare {
    fn into(self) -> FieldTable {
        let mut fields = FieldTable::default();
        if let Some(ttl) = self.ttl {
            fields.insert("x-message-ttl".into(), (ttl.as_millis() as u32).into() );
        }
        fields
    }
}

impl QueueDeclare {
    /// Creates the fields with TTL set in
    pub fn ttl(duration: Duration) -> QueueDeclare {
        Self { ttl: Some(duration) }
    }
}
