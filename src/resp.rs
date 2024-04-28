use std::{fmt::Display, num::ParseIntError, string::FromUtf8Error};

const SIMPLE_STRING_TYPE: u8 = b'+';

#[derive(Clone, Debug)]
pub struct SimpleString {
    s: String,
}

impl Display for SimpleString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.s)
    }
}

impl From<String> for SimpleString {
    fn from(value: String) -> Self {
        Self { s: value }
    }
}

impl From<&str> for SimpleString {
    fn from(value: &str) -> Self {
        Self {
            s: value.to_string(),
        }
    }
}

impl Into<String> for &SimpleString {
    fn into(self) -> String {
        self.s.clone()
    }
}

impl SimpleString {
    pub fn s(&self) -> &str {
        &self.s
    }

    fn serialize(&self) -> Vec<u8> {
        format!("{}{}\r\n", SIMPLE_STRING_TYPE as char, self.s).into_bytes()
    }
}

const SIMPLE_ERROR_TYPE: u8 = b'-';

#[derive(Clone, Debug)]
pub struct SimpleError {
    s: String,
}

impl Display for SimpleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.s)
    }
}

impl From<String> for SimpleError {
    fn from(value: String) -> Self {
        Self { s: value }
    }
}

impl From<&str> for SimpleError {
    fn from(value: &str) -> Self {
        Self {
            s: value.to_string(),
        }
    }
}

impl Into<String> for &SimpleError {
    fn into(self) -> String {
        self.s.clone()
    }
}

impl SimpleError {
    pub fn s(&self) -> &str {
        &self.s
    }

    fn serialize(&self) -> Vec<u8> {
        format!("{}{}\r\n", SIMPLE_ERROR_TYPE as char, self.s).into_bytes()
    }
}

const INTEGER_TYPE: u8 = b':';

#[derive(Clone, Debug)]
pub struct Integer {
    i: i64,
}

impl Display for Integer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.i)
    }
}

impl Into<i64> for &Integer {
    fn into(self) -> i64 {
        self.i
    }
}

impl From<i64> for Integer {
    fn from(value: i64) -> Self {
        Self { i: value }
    }
}

impl Integer {
    pub fn i(&self) -> i64 {
        self.i
    }

    fn serialize(&self) -> Vec<u8> {
        format!("{}{}\r\n", INTEGER_TYPE as char, self.to_string()).into_bytes()
    }
}

const BULK_STRING_TYPE: u8 = b'$';

#[derive(Clone, Debug)]
pub struct BulkString {
    is_null: bool,
    bytes: Vec<u8>,
}

impl Display for BulkString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.bytes)
    }
}

impl TryInto<String> for &BulkString {
    type Error = FromUtf8Error;
    fn try_into(self) -> Result<String, Self::Error> {
        String::from_utf8(self.bytes.clone())
    }
}

impl From<Vec<u8>> for BulkString {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            is_null: false,
            bytes,
        }
    }
}

impl BulkString {
    pub fn null() -> Self {
        Self {
            is_null: true,
            bytes: vec![],
        }
    }

    pub fn is_null(&self) -> bool {
        return self.is_null;
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn serialize(&self) -> Vec<u8> {
        let mut v = vec![BULK_STRING_TYPE];
        v.append(&mut self.bytes.len().to_string().into_bytes());
        v.append(&mut b"\r\n".into());
        v.append(&mut self.bytes.clone());
        v.append(&mut b"\r\n".into());

        v
    }
}

const ARRAY_TYPE: u8 = b'*';

#[derive(Clone, Debug)]
pub struct Array {
    is_null: bool,
    values: Vec<Value>,
}

impl Display for Array {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.values)
    }
}

impl From<Vec<Value>> for Array {
    fn from(values: Vec<Value>) -> Self {
        Self {
            is_null: false,
            values,
        }
    }
}

impl Array {
    pub fn null() -> Self {
        Self {
            is_null: true,
            values: vec![],
        }
    }

    pub fn is_null(&self) -> bool {
        self.is_null
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    fn serialize(&self) -> Vec<u8> {
        let mut v = vec![ARRAY_TYPE];
        v.append(&mut self.values.len().to_string().into_bytes());
        v.append(&mut b"\r\n".into());

        for val in &self.values {
            v.append(&mut val.serialize());
        }

        v
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(Integer),
    BulkString(BulkString),
    Array(Array),
}

impl Value {
    pub fn simple_string(&self) -> Option<&SimpleString> {
        match self {
            Self::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    pub fn simple_error(&self) -> Option<&SimpleError> {
        match self {
            Self::SimpleError(s) => Some(s),
            _ => None,
        }
    }

    pub fn integer(&self) -> Option<&Integer> {
        match self {
            Self::Integer(i) => Some(i),
            _ => None,
        }
    }

    pub fn bulk_string(&self) -> Option<&BulkString> {
        match self {
            Self::BulkString(bs) => Some(bs),
            _ => None,
        }
    }

    pub fn array(&self) -> Option<&Array> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        match self {
            Self::SimpleString(x) => x.serialize(),
            Self::SimpleError(x) => x.serialize(),
            Self::Integer(x) => x.serialize(),
            Self::BulkString(x) => x.serialize(),
            Self::Array(x) => x.serialize(),
        }
    }
}

pub struct Encoder {}

impl Encoder {
    pub fn encode(v: Value) -> Vec<u8> {
        v.serialize()
    }

    pub fn encode_simple_string(s: String) -> Vec<u8> {
        SimpleString::from(s).serialize()
    }

    pub fn encode_simple_error(s: String) -> Vec<u8> {
        SimpleError::from(s).serialize()
    }

    pub fn encode_integer(i: i64) -> Vec<u8> {
        Integer::from(i).serialize()
    }

    pub fn encode_bulk_string(b: &[u8]) -> Vec<u8> {
        BulkString::from(b.to_vec()).serialize()
    }

    pub fn encode_array(arr: Vec<Value>) -> Vec<u8> {
        Array::from(arr).serialize()
    }
}

#[derive(Debug)]
pub enum DecodeError {
    EmptyBytes,
    InvalidSimpleFormat,
    InvalidIntegerFormat,
    InvalidBulkFormat,
    BulkLenMismatch { given_len: usize, data_len: usize },
    InvalidArrayFormat,
    ArraySizeMismatch { given_size: usize, arr_size: usize },
    UnknownType { first_byte: u8 },
    ParseInt(ParseIntError),
    FromUTF8(FromUtf8Error),
}

impl From<ParseIntError> for DecodeError {
    fn from(err: ParseIntError) -> Self {
        Self::ParseInt(err)
    }
}

impl From<FromUtf8Error> for DecodeError {
    fn from(err: FromUtf8Error) -> Self {
        Self::FromUTF8(err)
    }
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyBytes => write!(f, "Empty bytes"),
            Self::InvalidSimpleFormat => write!(f, "Invalid simple format"),
            Self::InvalidIntegerFormat => write!(f, "Invalid integer format"),
            Self::InvalidBulkFormat => write!(f, "Invalid bulk format"),
            Self::BulkLenMismatch {
                given_len,
                data_len,
            } => write!(
                f,
                "Bulk length mismatch, given {given_len}, data {data_len}"
            ),
            Self::InvalidArrayFormat => write!(f, "Invalid array format"),
            Self::ArraySizeMismatch {
                given_size,
                arr_size,
            } => write!(
                f,
                "Array size mismatch, given {given_size}, data {arr_size}"
            ),
            Self::UnknownType { first_byte } => write!(f, "Unknown type {}", first_byte),
            Self::ParseInt(e) => write!(f, "Parse int error: {e}"),
            Self::FromUTF8(e) => write!(f, "From UTF8 error: {e}"),
        }
    }
}

pub struct Decoder {}

impl Decoder {
    pub fn decode(bytes: &[u8]) -> Result<Value, DecodeError> {
        let (val, _) = Self::decode_with_len(bytes)?;
        Ok(val)
    }

    fn decode_with_len(bytes: &[u8]) -> Result<(Value, usize), DecodeError> {
        if bytes.len() == 0 {
            return Err(DecodeError::EmptyBytes);
        }

        // Get first byte and match type
        match bytes.get(0).unwrap().clone() {
            SIMPLE_STRING_TYPE => {
                let (s, size) = Self::decode_simple_string(bytes)?;
                Ok((Value::SimpleString(s), size))
            }
            SIMPLE_ERROR_TYPE => {
                let (s, size) = Self::decode_simple_error(bytes)?;
                Ok((Value::SimpleError(s), size))
            }
            INTEGER_TYPE => {
                let (i, size) = Self::decode_integer(bytes)?;
                Ok((Value::Integer(i), size))
            }
            BULK_STRING_TYPE => {
                let (bs, size) = Self::decode_bulk_string(bytes)?;
                Ok((Value::BulkString(bs), size))
            }
            ARRAY_TYPE => {
                let (arr, size) = Self::decode_array(bytes)?;
                Ok((Value::Array(arr), size))
            }
            rest => Err(DecodeError::UnknownType { first_byte: rest }),
        }
    }

    /// Expects input to be in the form of `b"x<string>\r\n..."`, where x is the type of the RESP.
    ///
    /// Returns string and total bytes consumed.
    fn decode_to_string(bytes: &[u8]) -> Option<(String, usize)> {
        if let Some((b, size)) = read_until_crlf(bytes) {
            let s = match String::from_utf8(b[1..].into()) {
                Ok(s) => s,
                Err(_) => return None,
            };

            return Some((s, size));
        }

        None
    }

    /// Expects input to be in the form of `b"+<string>\r\n..."`.
    ///
    /// Returns string and total bytes consumed.
    fn decode_simple_string(bytes: &[u8]) -> Result<(SimpleString, usize), DecodeError> {
        let (s, size) = match Self::decode_to_string(bytes) {
            Some(r) => r,
            None => return Err(DecodeError::InvalidSimpleFormat),
        };
        Ok((s.into(), size))
    }

    /// Expects input to be in the form of `b"-<string>\r\n..."`.
    ///
    /// Returns string and total bytes consumed.
    fn decode_simple_error(bytes: &[u8]) -> Result<(SimpleError, usize), DecodeError> {
        let (s, size) = match Self::decode_to_string(bytes) {
            Some(r) => r,
            None => return Err(DecodeError::InvalidSimpleFormat),
        };
        Ok((s.into(), size))
    }

    /// Expects input to be in the form of `b":<integer>\r\n..."`.
    ///
    /// Returns integer and total bytes consumed.
    fn decode_integer(bytes: &[u8]) -> Result<(Integer, usize), DecodeError> {
        let (s, size) = match Self::decode_to_string(bytes) {
            Some(r) => r,
            None => return Err(DecodeError::InvalidIntegerFormat),
        };
        Ok((s.parse::<i64>()?.into(), size))
    }

    /// Expects input to be in the form of `b"$<len>\r\n<data>\r\n..."`.
    ///
    /// Returns bulk string and total bytes consumed.
    fn decode_bulk_string(bytes: &[u8]) -> Result<(BulkString, usize), DecodeError> {
        // Consume `b"$<len>\r\n"`
        let (bulk_str_len, bytes_consumed) =
            Self::decode_integer(bytes).map_err(|_| DecodeError::InvalidBulkFormat)?;
        let bulk_str_len: i64 = bulk_str_len.i();
        if bulk_str_len < 0 {
            return Ok((BulkString::null(), bytes_consumed));
        }

        // Consume `<data>\r\n`
        match read_until_crlf(&bytes[bytes_consumed..]) {
            Some((data, size)) => {
                if data.len() != bulk_str_len as usize {
                    return Err(DecodeError::BulkLenMismatch {
                        data_len: data.len(),
                        given_len: bulk_str_len as usize,
                    });
                }
                Ok((data.to_vec().into(), bytes_consumed + size))
            }
            None => Err(DecodeError::InvalidBulkFormat),
        }
    }

    /// Expects input to be in the form of `b"$<size>\r\n<element_1>\r\n<element2>\r\n..."`.
    ///
    /// Returns array and total bytes consumed.
    fn decode_array(bytes: &[u8]) -> Result<(Array, usize), DecodeError> {
        // Consume `b"$<size>\r\n"`
        let (arr_size, mut bytes_consumed) =
            Self::decode_integer(bytes).map_err(|_| DecodeError::InvalidArrayFormat)?;
        let arr_size: i64 = arr_size.i();
        if arr_size < 0 {
            return Ok((Array::null(), bytes_consumed));
        }

        let mut values = vec![];
        for _ in 0..arr_size {
            let (val, len) = Self::decode_with_len(&bytes[bytes_consumed..])?;
            values.push(val);
            bytes_consumed += len;
        }

        Ok((Array::from(values), bytes_consumed))
    }
}

/// Read until the first CRLF.
///
/// Returns size of read buffer as well.
fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    return None;
}

mod util_test {
    use super::*;
}

#[cfg(test)]
mod decoder_test {
    use super::*;

    #[test]
    fn decode_simple_string() {
        let resp = Decoder::decode(b"+OK\r\n").expect("Decode simple string unexpected error");
        match resp {
            Value::SimpleString(s) => assert_eq!(s.to_string(), "OK"),
            any => panic!("Wrong type for decode simple string: {:?}", any),
        }
    }

    #[test]
    fn decode_simple_error() {
        let resp =
            Decoder::decode(b"-ERR something\r\n").expect("Decode simple error unexpected error");
        match resp {
            Value::SimpleError(s) => assert_eq!(s.to_string(), "ERR something"),
            any => panic!("Wrong type for decode simple error: {:?}", any),
        }
    }

    #[test]
    fn decode_integer() {
        let resp = Decoder::decode(b":123\r\n").expect("Decode integer unexpected error");
        match resp {
            Value::Integer(i) => assert_eq!(i.i(), 123),
            any => panic!("Wrong type for decode integer: {:?}", any),
        }
    }

    #[test]
    fn decode_bulk_string() {
        let resp = Decoder::decode(b"$4\r\nHell\r\n").expect("Decode bulk string unexpected error");
        match resp {
            Value::BulkString(bs) => assert_eq!(bs.bytes(), b"Hell"),
            any => panic!("Wrong type for decode bulk string: {:?}", any),
        }
    }

    #[test]
    fn decode_bulk_string_mismatch_len() {
        let err = Decoder::decode(b"$3\r\nHell\r\n").expect_err("Decode bulk string no error");
        match err {
            DecodeError::BulkLenMismatch { .. } => (),
            any => panic!("Wrong error for decode bulk string: {:?}", any),
        }
    }

    #[test]
    fn decode_bulk_string_invalid_format() {
        let err = Decoder::decode(b"$Liberty\r\n").expect_err("Decode bulk string no error");
        match err {
            DecodeError::InvalidBulkFormat => (),
            any => panic!("Wrong error for decode bulk string: {:?}", any),
        }
    }

    #[test]
    fn decode_array() {
        let resp =
            Decoder::decode(b"*2\r\n:12\r\n+Yea\r\n").expect("Decode array unexpected error");
        match resp {
            Value::Array(arr) => {
                let mut iter = arr.values().iter();
                assert_eq!(iter.next().unwrap().integer().unwrap().i(), 12);
                assert_eq!(iter.next().unwrap().simple_string().unwrap().s(), "Yea");
            }
            _ => panic!("Wrong type for decode array"),
        }
    }

    #[test]
    fn decode_array_nested() {
        let resp = Decoder::decode(
            b"*2\r\n*3\r\n:12\r\n+Yea\r\n-Oopsie\r\n*2\r\n$5\r\nHello\r\n$4\r\nGGWP\r\n",
        )
        .expect("Decode array unexpected error");
        match resp {
            Value::Array(arr) => {
                let first_values = arr.values().get(0).unwrap().array().unwrap().values();
                assert_eq!(first_values.get(0).unwrap().integer().unwrap().i(), 12);
                assert_eq!(
                    first_values.get(1).unwrap().simple_string().unwrap().s(),
                    "Yea"
                );
                assert_eq!(
                    first_values.get(2).unwrap().simple_error().unwrap().s(),
                    "Oopsie"
                );

                let second_values = arr.values().get(1).unwrap().array().unwrap().values();
                assert_eq!(
                    second_values.get(0).unwrap().bulk_string().unwrap().bytes(),
                    "Hello".as_bytes()
                );
                assert_eq!(
                    second_values.get(1).unwrap().bulk_string().unwrap().bytes(),
                    "GGWP".as_bytes()
                );
            }
            _ => panic!("Wrong type for decode array"),
        }
    }
}
