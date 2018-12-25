// use lazy_static::lazy_staitc;
use crate::com::BeeError;
use bytes::{BufMut, BytesMut};
use failure::Error;

pub const REDIS_RDB_6BITLEN: u8 = 0;
pub const REDIS_RDB_14BITLEN: u8 = 1;
pub const REDIS_RDB_32BITLEN: u8 = 0x80;
pub const REDIS_RDB_64BITLEN: u8 = 0x81;
pub const REDIS_RDB_ENCVAL: u8 = 3;

pub const REDIS_RDB_OPCODE_MODULE_AUX: u8 = 247;
pub const REDIS_RDB_OPCODE_IDLE: u8 = 248;
pub const REDIS_RDB_OPCODE_FREQ: u8 = 249;
pub const REDIS_RDB_OPCODE_AUX: u8 = 250;
pub const REDIS_RDB_OPCODE_RESIZEDB: u8 = 251;
pub const REDIS_RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
pub const REDIS_RDB_OPCODE_EXPIRETIME: u8 = 253;
pub const REDIS_RDB_OPCODE_SELECTDB: u8 = 254;
pub const REDIS_RDB_OPCODE_EOF: u8 = 255;

pub const REDIS_RDB_TYPE_STRING: u8 = 0;
pub const REDIS_RDB_TYPE_LIST: u8 = 1;
pub const REDIS_RDB_TYPE_SET: u8 = 2;
pub const REDIS_RDB_TYPE_ZSET: u8 = 3;
pub const REDIS_RDB_TYPE_HASH: u8 = 4;
pub const REDIS_RDB_TYPE_ZSET_2: u8 = 5; // ZSET version 2 with doubles stored in binary.
pub const REDIS_RDB_TYPE_MODULE: u8 = 6;
pub const REDIS_RDB_TYPE_MODULE_2: u8 = 7;
pub const REDIS_RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub const REDIS_RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub const REDIS_RDB_TYPE_SET_INTSET: u8 = 11;
pub const REDIS_RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub const REDIS_RDB_TYPE_HASH_ZIPLIST: u8 = 13;
pub const REDIS_RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub const REDIS_RDB_TYPE_STREAM_LISTPACKS: u8 = 15;

pub const REDIS_RDB_ENC_INT8: u8 = 0;
pub const REDIS_RDB_ENC_INT16: u8 = 1;
pub const REDIS_RDB_ENC_INT32: u8 = 2;
pub const REDIS_RDB_ENC_LZF: u8 = 3;

pub const REDIS_RDB_MODULE_OPCODE_EOF: u8 = 0; //  End of module value.
pub const REDIS_RDB_MODULE_OPCODE_SINT: u8 = 1;
pub const REDIS_RDB_MODULE_OPCODE_UINT: u8 = 2;
pub const REDIS_RDB_MODULE_OPCODE_FLOAT: u8 = 3;
pub const REDIS_RDB_MODULE_OPCODE_DOUBLE: u8 = 4;
pub const REDIS_RDB_MODULE_OPCODE_STRING: u8 = 5;

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

pub struct RDBParser {
    dtype: u8,

    version: u64,
    db: u64,
    expire: Option<u64>,
    idle: u64,
    freq: u8,
}

impl RDBParser {
    #[inline]
    fn read_u8(src: &mut BytesMut) -> u8 {
        let dtype = src[0];
        src.advance(1);
        dtype
    }

    #[inline]
    fn read_length(&mut self, src: &mut BytesMut) -> Result<u64, Error> {
        unimplemented!()
    }

    #[inline]
    fn read_expire(&mut self, src: &mut BytesMut, base: u64) -> Result<u64, Error> {
        unimplemented!()
    }

    #[inline]
    fn read_string(&mut self, src: &mut BytesMut) -> Result<Vec<u8>, Error> {
        unimplemented!()
    }

    #[inline]
    fn read_module(&mut self, src: &mut BytesMut) -> Result<(), Error> {
        unimplemented!()
    }

    fn parse(&mut self, src: &mut BytesMut) -> Result<Entry, Error> {
        let magic_str = src.split_off(5);
        let vbytes = src.split_off(4);
        self.version = String::from_utf8_lossy(&vbytes).parse::<u64>()?;

        loop {
            self.expire = None;
            self.idle = 0;
            self.freq = 0;
            self.dtype = Self::read_u8(src);
            if self.dtype == REDIS_RDB_OPCODE_EXPIRETIME {
                self.expire = Some(self.read_expire(src, 1)?);
                self.dtype = Self::read_u8(src);
            } else if self.dtype == REDIS_RDB_OPCODE_EXPIRETIME_MS {
                self.expire = Some(self.read_expire(src, 1000)?);
                self.dtype = Self::read_u8(src);
            }

            if self.dtype == REDIS_RDB_OPCODE_IDLE {
                self.idle = self.read_length(src)?;
                self.dtype = Self::read_u8(src);
            }

            if self.dtype == REDIS_RDB_OPCODE_FREQ {
                self.freq = Self::read_u8(src);
                self.dtype = Self::read_u8(src);
            }

            if self.dtype == REDIS_RDB_OPCODE_SELECTDB {
                self.db = self.read_length(src)?;
                continue;
            }

            if self.dtype == REDIS_RDB_OPCODE_AUX {
                let _aux_key = self.read_string(src)?;
                let _aux_value = self.read_string(src)?;
                continue;
            }

            if self.dtype == REDIS_RDB_OPCODE_RESIZEDB {
                let _db_size = self.read_length(src)?;
                let _expire_size = self.read_length(src)?;
                continue
            }

            if self.dtype == REDIS_RDB_OPCODE_MODULE_AUX {
                self.read_module(src)?;
                continue
            }

            if self.dtype == REDIS_RDB_OPCODE_EOF {
                if self.version >= 5 {
                    let _ = src.split_off(8);
                }
                break;
            }

            // if self.db_filter(self.db) {
            //     self.read_kv(src)?;
            // }

        }

        Ok(Entry{})
    }
}

pub struct Entry {}
