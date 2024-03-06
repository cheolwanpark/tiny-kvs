use serde::{Serialize, Deserialize};

#[derive(Clone, Copy)]
pub struct Bytes<const N: usize>(pub [u8; N]);

impl<const N: usize> Serialize for Bytes<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de, const N: usize> Deserialize<'de> for Bytes<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf: Vec<u8> = serde::Deserialize::deserialize(deserializer)?;
        let mut array = [0u8; N];
        let bytes = &buf[..array.len()]; // panics if not enough data
        array.copy_from_slice(bytes);
        Ok(Bytes(array))
    }
}