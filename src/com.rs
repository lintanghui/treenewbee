use failure::Error;

#[derive(Debug, Fail)]
pub enum BeeError {
    #[fail(display = "invalid MagicString : {:?}", head)]
    BadMagicString { head: Vec<u8> },
    #[fail(display = "invalid version: {:?}", version)]
    BadVersion { version: usize },
}
