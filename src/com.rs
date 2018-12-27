use failure::Error;
use lzf;
use std::convert::From;


#[derive(Debug, Fail)]
pub enum BeeError {
    #[fail(display = "more data need in buffer")]
    More,
    #[fail(display = "invalid MagicString : {:?}", head)]
    BadMagicString { head: Vec<u8> },
    #[fail(display = "invalid version: {:?}", version)]
    BadVersion { version: usize },
    #[fail(display = "read rdb but not a length encoding number")]
    NotNumber,
    #[fail(display = "read rdb but not a length encoding LZF type")]
    NotLZF,
    #[fail(display = "decompress lzf data fail")]
    LzfError(lzf::LzfError),
}


impl From<lzf::LzfError> for BeeError {
    fn from(oe: lzf::LzfError) -> BeeError {
        BeeError::LzfError(oe)
    }
}
