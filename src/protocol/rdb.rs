#![allow(unused)]
// use lazy_static::lazy_staitc;
use crate::com::BeeError;
use bytes::{BufMut, BytesMut};
use failure::Error;
use lzf;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use std::collections::{HashMap, HashSet};

pub const RDB_6BITLEN: u8 = 0;
pub const RDB_14BITLEN: u8 = 1;
pub const RDB_32BITLEN: u8 = 0x80;
pub const RDB_64BITLEN: u8 = 0x81;
pub const RDB_ENCVAL: u8 = 3;

pub const RDB_OPCODE_MODULE_AUX: u8 = 247;
pub const RDB_OPCODE_IDLE: u8 = 248;
pub const RDB_OPCODE_FREQ: u8 = 249;
pub const RDB_OPCODE_AUX: u8 = 250;
pub const RDB_OPCODE_RESIZEDB: u8 = 251;
pub const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
pub const RDB_OPCODE_EXPIRETIME: u8 = 253;
pub const RDB_OPCODE_SELECTDB: u8 = 254;
pub const RDB_OPCODE_EOF: u8 = 255;

pub const RDB_TYPE_STRING: u8 = 0;
pub const RDB_TYPE_LIST: u8 = 1;
pub const RDB_TYPE_SET: u8 = 2;
pub const RDB_TYPE_ZSET: u8 = 3;
pub const RDB_TYPE_HASH: u8 = 4;
pub const RDB_TYPE_ZSET_2: u8 = 5; // ZSET version 2 with doubles stored in binary.
pub const RDB_TYPE_MODULE: u8 = 6;
pub const RDB_TYPE_MODULE_2: u8 = 7;
pub const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub const RDB_TYPE_SET_INTSET: u8 = 11;
pub const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
pub const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;

pub const RDB_ENC_INT8: u8 = 0;
pub const RDB_ENC_INT16: u8 = 1;
pub const RDB_ENC_INT32: u8 = 2;
pub const RDB_ENC_LZF: u8 = 3;

pub const RDB_MODULE_OPCODE_EOF: u8 = 0; //  End of module value.
pub const RDB_MODULE_OPCODE_SINT: u8 = 1;
pub const RDB_MODULE_OPCODE_UINT: u8 = 2;
pub const RDB_MODULE_OPCODE_FLOAT: u8 = 3;
pub const RDB_MODULE_OPCODE_DOUBLE: u8 = 4;
pub const RDB_MODULE_OPCODE_STRING: u8 = 5;

// DATA_TYPE_MAPPING = {
//     0: "string",
//     1: "list",
//     2: "set",
//     3: "sortedset",
//     4: "hash",
//     5: "sortedset",
//     6: "module",
//     7: "module",
//     9: "hash",
//     10: "list",
//     11: "set",
//     12: "sortedset",
//     13: "hash",
//     14: "list",
//     15: "stream"
// }

macro_rules! more {
    ($e: expr) => {
        more!($e, 0);
    };
    ($e: expr, 0) => {
        if $e.is_empty() {
            return Err(crate::com::BeeError::More.into());
        }
    };
    ($e: expr, $s:expr) => {
        if $e.len() < $s {
            return Err(crate::com::BeeError::More.into());
        }
    };
}

#[derive(Clone, Debug, Copy)]
enum State {
    Head,
    Data,
    EOF,
}

pub struct RDBParser {
    state: State,
    version: usize,
    db: u64,
}

impl RDBParser {
    fn read_head(&mut self, src: &mut BytesMut) -> Result<(), Error> {
        more!(src, 9);
        if b"REDIS" != &src[..5] {
            return Err(BeeError::BadMagicString {
                head: src[..5].to_vec(),
            }
            .into());
        }
        let version = String::from_utf8_lossy(&src[5..9]).parse::<usize>()?;
        if version < 1 || version > 9 {
            return Err(BeeError::BadVersion { version: version }.into());
        }
        self.version = version;
        src.advance(9);
        Ok(())
    }

    #[inline]
    fn read_expire(&mut self, src: &mut BytesMut) -> Result<Option<u64>, Error> {
        more!(src, 4);
        let expire = LittleEndian::read_u32(&src);
        Ok(Some(expire as u64))
    }

    fn read_expire_ms(&mut self, src: &mut BytesMut) -> Result<Option<u64>, Error> {
        more!(src, 8);
        let expire = LittleEndian::read_u64(&src);
        Ok(Some(expire))
    }

    fn read_object(&mut self, src: &mut BytesMut, dtype: u8) -> Result<RedisObject, Error> {
        unimplemented!();
    }

    fn read_body(&mut self, src: &mut BytesMut) -> Result<Option<Entry>, Error> {
        more!(src);
        let mut expire = None;
        // let mut idle = None;
        // let mut freq = None;
        let key: BytesMut;
        let data: RedisObject;
        loop {
            let dtype = self.read_u8(src)?;
            if dtype == RDB_OPCODE_EXPIRETIME {
                // offset 4
                expire = self.read_expire(src)?.map(|x| (x as u64) * 1000);
                continue;
            } else if dtype == RDB_OPCODE_EXPIRETIME_MS {
                // offset 4
                expire = self.read_expire_ms(src)?;
                continue;
            }

            if dtype == RDB_OPCODE_IDLE {
                // offset dynamic
                let _idle = self.read_length(src)?;
                continue;
            }

            if dtype == RDB_OPCODE_FREQ {
                // offset 1
                let _freq = self.read_u8(src)?;
                continue;
            }

            if dtype == RDB_OPCODE_SELECTDB {
                // select db
                // TODO: should convert it as select db
                self.db = self.read_length(src)?;
                continue;
            }

            if dtype == RDB_OPCODE_AUX {
                let _aux_key = self.read_string(src)?;
                let _aux_val = self.read_string(src)?;
                // TODO: should report
                continue;
            }

            if dtype == RDB_OPCODE_RESIZEDB {
                let _db_size = self.read_length(src)?;
                let _expire_size = self.read_length(src)?;
                // TODO: should report
                continue;
            }

            if dtype == RDB_OPCODE_MODULE_AUX {
                self.read_module(src)?;
                continue;
            }

            if dtype == RDB_OPCODE_EOF {
                self.state = State::EOF;
                return Ok(None);
            }

            key = self.read_string(src)?;
            data = self.read_object(src, dtype)?;
            let entry = Entry {
                key: key,
                expire: expire,
                data: data,
            };
            return Ok(Some(entry));
        }
    }

    fn read_eof(&mut self, src: &mut BytesMut) -> Result<(), Error> {
        if self.version >= 5 {
            more!(src, 5);
            src.advance(5);
        }
        more!(src, 8);
        src.advance(8);
        Ok(())
    }

    pub fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Entry>, Error> {
        loop {
            match self.state {
                State::Head => self.read_head(src)?,
                State::Data => return self.read_body(src),
                State::EOF => {
                    self.read_eof(src)?;
                    return Ok(None);
                }
            }
        }
    }

    #[inline]
    fn read_u8(&mut self, src: &mut BytesMut) -> Result<u8, Error> {
        more!(src);
        let dtype = src[0];
        src.advance(1);
        Ok(dtype)
    }

    fn read_length(&mut self, src: &mut BytesMut) -> Result<u64, Error> {
        more!(src);
        let flag = (src[0] & 0xc0) >> 6;
        if flag == RDB_6BITLEN {
            let ret = Ok((src[0] & 0x3f) as u64);
            src.advance(1);
            return ret;
        } else if flag == RDB_14BITLEN {
            more!(src, 2);
            let mut val = src[1] as u64;
            val &= ((src[0] & 0x3f) as u64) << 8;
            src.advance(2);
            return Ok(val);
        } else if flag == RDB_32BITLEN {
            more!(src, 4 + 1);
            let val = BigEndian::read_u32(&src[1..]);
            src.advance(5);
            return Ok(val as u64);
        } else if flag == RDB_64BITLEN {
            more!(src, 8 + 1);
            let val = BigEndian::read_u64(&src[1..]);
            src.advance(9);
            return Ok(val);
        }
        Err(BeeError::NotNumber.into())
    }

    #[inline]
    fn read_string(&mut self, src: &mut BytesMut) -> Result<BytesMut, Error> {
        match self
            .read_length(src)
            .map_err(|err| err.downcast::<BeeError>())
        {
            Ok(len) => {
                let len = len as usize;
                // length encoding string
                more!(src, len);
                let buf = src.split_off(len);
                Ok(buf)
            }
            Err(Ok(_be)) => {
                // encoded buf string or encoded int
                let flag = src[0] & 0x3f;
                return self.read_string_enc(flag, src);
            }
            Err(Err(err)) => Err(err),
        }
    }

    fn read_string_enc(&mut self, flag: u8, src: &mut BytesMut) -> Result<BytesMut, Error> {
        match flag {
            RDB_ENC_INT8 => {
                more!(src, 1 + 1);
                let mut buf = src.split_off(2);
                buf.advance(1);
                Ok(buf)
            }
            RDB_ENC_INT16 => {
                more!(src, 1 + 2);
                let mut buf = src.split_off(3);
                buf.advance(1);
                Ok(buf)
            }
            RDB_ENC_INT32 => {
                more!(src, 1 + 2);
                let mut buf = src.split_off(5);
                buf.advance(1);
                Ok(buf)
            }
            RDB_ENC_LZF => self.read_string_enc_lzf(src),
            _ => unreachable!(),
        }
    }

    fn read_string_enc_lzf(&mut self, src: &mut BytesMut) -> Result<BytesMut, Error> {
        more!(src, 2);
        src.advance(1);
        let blen = self.read_length(src)? as usize;
        let clen = self.read_length(src)? as usize;
        more!(src, blen);
        let data = lzf::decompress(&src[..blen], clen).map_err(|err| BeeError::from(err))?;
        Ok(data.into())
    }

    #[inline]
    fn read_module(&mut self, _src: &mut BytesMut) -> Result<(), Error> {
        // TODO: impl it
        unimplemented!()
    }
}

pub struct Entry {
    key: BytesMut,
    expire: Option<u64>,
    data: RedisObject,
}

pub enum RedisObject {
    Raw(BytesMut),
    List(Vec<BytesMut>),
    Set(HashSet<BytesMut>),
    ZSet(HashSet<ZSetPair>),
    ZSet2(HashSet<ZSetPair2>),
    Hash(HashMap<BytesMut, BytesMut>),
}

pub struct ZSetPair {
    score: f64,
    value: BytesMut,
}

pub struct ZSetPair2 {
    score: f64,
    value: f64,
}
