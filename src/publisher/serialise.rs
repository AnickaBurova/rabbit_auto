use std::borrow::Cow;

/// Trait used to serialise the item in to byte vector data.
/// This is used as payload for the message.
pub trait Serialise {
    /// Gets the serialised data of the item.
    fn serialise(data: &Self) -> Cow<[u8]>;
}

impl Serialise for String {
    fn serialise(data: &Self) -> Cow<[u8]> {
        data.as_bytes().into()
    }
}

impl Serialise for Vec<u8> {
    fn serialise(data: &Self) -> Cow<[u8]> {
        data.as_slice().into()
    }
}
