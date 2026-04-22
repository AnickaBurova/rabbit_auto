use std::borrow::Cow;

/// Converts a message into the raw bytes that will be sent as the AMQP payload.
///
/// Implement this for your message type and pass it as the `Item` type parameter
/// to [`PublishWrapper`](crate::publisher::PublishWrapper).
///
/// Returning a zero-length slice causes the publish to be skipped silently.
///
/// # Provided implementations
///
/// | Type | Behaviour |
/// |------|-----------|
/// | `String` | Borrows the UTF-8 bytes without copying. |
/// | `Vec<u8>` | Borrows the raw bytes without copying. |
///
/// # Example — JSON via serde
///
/// ```rust
/// use std::borrow::Cow;
/// use rabbit_auto::publisher::Serialise;
///
/// struct OrderPlaced { order_id: u64 }
///
/// impl Serialise for OrderPlaced {
///     fn serialise(data: &Self) -> Cow<'_, [u8]> {
///         // replace with your actual serialiser, e.g. serde_json::to_vec
///         format!(r#"{{"order_id":{}}}"#, data.order_id)
///             .into_bytes()
///             .into()
///     }
/// }
/// ```
pub trait Serialise {
    /// Returns the wire representation of this message.
    fn serialise(data: &Self) -> Cow<'_, [u8]>;
}

impl Serialise for String {
    fn serialise(data: &Self) -> Cow<'_, [u8]> {
        data.as_bytes().into()
    }
}

impl Serialise for Vec<u8> {
    fn serialise(data: &Self) -> Cow<'_, [u8]> {
        data.as_slice().into()
    }
}
