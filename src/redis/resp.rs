use std::{fmt::Display, io, num::ParseIntError, str::FromStr, string::FromUtf8Error};

use derive_more::{Display, Into};
use thiserror::Error;

/// EncodeError is returned by Encoder when there are any issues with encoding.
#[derive(Debug, Error)]
pub enum EncodeError {
    /// Io is returned if there are problems with writing to `io::Write`.
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[enum_delegate::register]
trait Encoder {
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError>;
}

#[derive(Debug, Clone, Error)]
pub enum DecodeError {
    #[error("empty bytes")]
    EmptyBytes,

    #[error("invalid format")]
    InvalidFormat,

    #[error("length mismatch, given {given_len}, actual {actual_len}")]
    LenMismatch { given_len: usize, actual_len: usize },

    #[error("unknown type {first_byte}")]
    UnknownType { first_byte: u8 },

    #[error(transparent)]
    ParseInt(#[from] ParseIntError),

    #[error(transparent)]
    FromUtf8(#[from] FromUtf8Error),
}

trait Decoder {
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized;
}

/// A valid starting token from Redis Serialization Protocol
#[derive(Debug, Eq, PartialEq, Clone)]
#[repr(u8)]
pub enum Token {
    Star = b'*',   // Array
    Dollar = b'$', // BulkString
    Plus = b'+',   // SimpleString
    Minus = b'-',  // SimpleError
    Colon = b':',  // Integer
}

impl Into<char> for Token {
    fn into(self) -> char {
        self as u8 as char
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c: char = self.clone().into();
        write!(f, "{c}")
    }
}

impl Token {
    pub fn from(c: char) -> Option<Self> {
        match c {
            '*' => Some(Self::Star),
            '$' => Some(Self::Dollar),
            '+' => Some(Self::Plus),
            '-' => Some(Self::Minus),
            ':' => Some(Self::Colon),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Into, Display)]
pub struct SimpleString {
    s: String,
}

impl Into<String> for &SimpleString {
    fn into(self) -> String {
        self.s.clone()
    }
}

impl From<String> for SimpleString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SimpleString {
    fn from(value: &str) -> Self {
        Self::new(value.to_owned())
    }
}

impl Encoder for SimpleString {
    /// Encodes string formatted as `b"+<string>\r\n"`.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if there are no issues with encoding and writing.
    /// - `EncodeError::...` if there were encoding errors, see the enum variants in order
    ///     to understand what is the specific error.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        write!(buf, "{}{}\r\n", Token::Plus, self.s)?;
        Ok(())
    }
}

impl Decoder for SimpleString {
    /// Decodes bytes into SimpleString.
    /// Expects input to be in the form of `b"+<string>\r\n..."`.
    ///
    /// # Returns
    ///
    /// - `Ok((SimpleString, usize))` if there are no issues with decoding. The usize represents total bytes read
    ///     from the buffer while decoding.
    /// - `DecodeError::...` if there were some decoding errors, see the enum variants in order to
    ///     understand what is the specific error.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        let (s, size) = decode_to_string(buf)?;
        Ok((s.into(), size))
    }
}

impl SimpleString {
    pub fn new(s: String) -> Self {
        Self { s }
    }

    /// Returns SimpleString as string.
    pub fn as_str(&self) -> &str {
        &self.s
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Display, Into)]
pub struct SimpleError {
    s: String,
}

impl Into<String> for &SimpleError {
    fn into(self) -> String {
        self.s.clone()
    }
}

impl From<String> for SimpleError {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SimpleError {
    fn from(value: &str) -> Self {
        Self::new(value.to_owned())
    }
}

impl Encoder for SimpleError {
    /// Encodes SimpleError formatted as `b"-<string>\r\n`.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if there are no issues with encoding and writing.
    /// - `EncodeError::...` if there were encoding errors, see the enum variants in order
    ///     to understand what is the specific error.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        write!(buf, "{}{}\r\n", Token::Minus, self.s)?;
        Ok(())
    }
}

impl Decoder for SimpleError {
    /// Decodes bytes into SimpleError.
    /// Expects input to be in the form of `b"-<string>\r\n..."`.
    ///
    /// # Returns
    ///
    /// - `Ok((SimpleError, usize))` if there are no issues with decoding. The usize represents total bytes read
    ///     from the buffer while decoding.
    /// - `DecodeError::...` if there were some decoding errors, see the enum variants in order to
    ///     understand what is the specific error.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        let (s, size) = decode_to_string(buf)?;
        Ok((s.into(), size))
    }
}

impl SimpleError {
    pub fn new(s: String) -> Self {
        Self { s }
    }

    /// Returns SimpleError as string.
    pub fn as_str(&self) -> &str {
        &self.s
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Display, Into)]
pub struct Integer {
    i: i64,
}

impl Into<i64> for &Integer {
    fn into(self) -> i64 {
        self.i
    }
}

impl From<i64> for Integer {
    fn from(value: i64) -> Self {
        Self::new(value)
    }
}

impl Encoder for Integer {
    /// Encodes Integer formatted as `b":<integer>\r\n"`.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if there are no issues with encoding and writing.
    /// - `EncodeError::...` if there were encoding errors, see the enum variants in order
    ///     to understand what is the specific error.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        write!(buf, "{}{}\r\n", Token::Colon, self.i)?;
        Ok(())
    }
}

impl Decoder for Integer {
    /// Decodes bytes into Integer.
    /// Expects input to be in the form of `b":<integer>\r\n..."`.
    ///
    /// # Returns
    ///
    /// - `Ok((Integer, usize))` if there are no issues with decoding. The usize represents total bytes read
    ///     from the buffer while decoding.
    /// - `DecodeError::...` if there were some decoding errors, see the enum variants in order to
    ///     understand what is the specific error.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        let (i, size) = decode_to_i64(buf)?;
        Ok((i.into(), size))
    }
}

impl Integer {
    pub fn new(i: i64) -> Self {
        Self { i }
    }

    /// Returns Integer as int64.
    pub fn as_int(&self) -> i64 {
        self.i
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BulkString {
    bytes: Option<Vec<u8>>,
}

impl Display for BulkString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.bytes)
    }
}

impl From<Vec<u8>> for BulkString {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl From<&str> for BulkString {
    fn from(s: &str) -> Self {
        Self::new(s.as_bytes().to_vec())
    }
}

impl From<String> for BulkString {
    fn from(s: String) -> Self {
        Self::new(s.as_bytes().to_vec())
    }
}

impl FromStr for BulkString {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

impl Encoder for BulkString {
    /// Encodes BulkString formatted as `b"$<len>\r\n<data>\r\n"`
    ///
    /// # Returns
    ///
    /// - `Ok(())` if there are no issues with encoding and writing.
    /// - `EncodeError::...` if there were encoding errors, see the enum variants in order
    ///     to understand what is the specific error.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        let bytes = match &self.bytes {
            Some(b) => b,
            None => {
                // Null BulkString
                write!(buf, "{}-1\r\n", Token::Dollar)?;
                return Ok(());
            }
        };

        write!(buf, "{}{}\r\n", Token::Dollar, bytes.len())?;
        buf.write_all(&bytes)?;
        write!(buf, "\r\n")?;
        Ok(())
    }
}

impl Decoder for BulkString {
    /// Decodes bytes into BulkString.
    /// Expects input to be in the form of `b"$<len>\r\n<data>\r\n..."`.
    ///
    /// # Returns
    ///
    /// - `Ok((BulkString, usize))` if there are no issues with decoding. The usize represents total bytes read
    ///     from the buffer while decoding.
    /// - `DecodeError::...` if there were some decoding errors, see the enum variants in order to
    ///     understand what is the specific error.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        // Consume `b"$<len>\r\n"`
        let (bulk_str_len, bytes_consumed) = decode_to_i64(buf)?;
        if bulk_str_len < 0 {
            return Ok((BulkString::null(), bytes_consumed));
        }

        // Consume `<data>\r\n`
        match read_until_crlf(&buf[bytes_consumed..]) {
            Some((data, size)) => {
                if data.len() != bulk_str_len as usize {
                    return Err(DecodeError::LenMismatch {
                        actual_len: data.len(),
                        given_len: bulk_str_len as usize,
                    });
                }
                Ok((data.to_vec().into(), bytes_consumed + size))
            }
            None => Err(DecodeError::InvalidFormat),
        }
    }
}

impl BulkString {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes: Some(bytes) }
    }

    pub fn null() -> Self {
        Self { bytes: None }
    }

    /// Returns BulkString as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match &self.bytes {
            Some(b) => Some(b),
            None => None,
        }
    }

    /// Returns BulkString as string if it can be encoded into a string.
    /// Otherwise returns None.
    pub fn as_str(&self) -> Option<String> {
        if let Some(bytes) = self.as_bytes() {
            return match String::from_utf8(bytes.to_vec()) {
                Ok(s) => Some(s),
                Err(_) => None,
            };
        }

        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Array {
    values: Option<Vec<Value>>,
}

impl Display for Array {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.values)
    }
}

impl From<Vec<Value>> for Array {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

impl Array {
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            values: Some(values),
        }
    }

    pub fn null() -> Self {
        Self { values: None }
    }

    /// Returns list of Values contained in the Array.
    pub fn values(&self) -> Option<&[Value]> {
        match &self.values {
            Some(values) => Some(values),
            None => None,
        }
    }
}

impl Encoder for Array {
    /// Encodes Array formatted as`b"$<size>\r\n<element_1>\r\n<element2>\r\n..."`.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if there are no issues with encoding and writing.
    /// - `EncodeError::...` if there were encoding errors, see the enum variants in order
    ///     to understand what is the specific error.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        let values = match &self.values {
            Some(v) => v,
            None => {
                write!(buf, "{}-1\r\n", Token::Star)?;
                return Ok(());
            }
        };

        write!(buf, "{}{}\r\n", Token::Star, values.len())?;
        for val in values {
            val._encode(buf)?;
        }

        Ok(())
    }
}

impl Decoder for Array {
    /// Decodes bytes into Array.
    /// Expects input to be in the form of `b"$<size>\r\n<element_1>\r\n<element2>\r\n..."`.
    ///
    /// # Returns
    ///
    /// - `Ok((Array, usize))` if there are no issues with decoding. The usize represents total bytes read
    ///     from the buffer while decoding.
    /// - `DecodeError::...` if there were some decoding errors, see the enum variants in order to
    ///     understand what is the specific error.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        // Consume `b"$<size>\r\n"`
        let (arr_size, mut bytes_consumed) = decode_to_i64(buf)?;
        if arr_size < 0 {
            return Ok((Array::null(), bytes_consumed));
        }

        // Consume the rest of elements
        let mut values = vec![];
        for _ in 0..arr_size {
            let (val, len) = Value::decode_with_len(&buf[bytes_consumed..])?;
            values.push(val);
            bytes_consumed += len;
        }

        Ok((Array::from(values), bytes_consumed))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Display)]
#[enum_delegate::implement(Encoder)]
pub enum Value {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(Integer),
    BulkString(BulkString),
    Array(Array),
}

impl Value {
    /// Encodes the Value by writing its RESP byte-form into writers.
    /// See the respective enum variants for the exact RESP format.
    ///
    /// # Arguments
    ///
    /// - `buf`: A mutable reference to an implementation of the `io::Write` trait. The bytes will be
    ///     written into this buffer.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if there are no problems with the encoding and writing.
    /// - `EncodeError::...` if there were encoding errors, see the enum variants in order
    ///     to understand what is the specific error.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::io::BufWriter;
    /// use redis_starter_rust::redis::resp;
    ///
    /// let value = resp::Value::BulkString(resp::BulkString::from("Something"));
    /// let mut buf = BufWriter::new(Vec::new());
    /// match value.encode(&mut buf) {
    ///     Ok(_) => println!("All good!"),
    ///     Err(e) => println!("Oh no, something wrong: {e}"),
    /// }
    /// ```
    ///
    /// In the above example, we create a BulkString Value and encode it into a BufWriter.
    pub fn encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        self._encode(buf)
    }

    /// Decodes the bytes according to RESP into Value.
    ///
    /// # Arguments
    ///
    /// - `buf`: A reference to the bytes to be decoded, which should be in RESP format.
    ///
    /// # Returns
    ///
    /// - `Ok(Value)` if there are no problems with the decoding. The `Value` represents the decoded
    ///     value of the bytes.
    /// - `DecodeError::...` if there were some decoding errors, see the enum variants in order to
    ///     understand what is the specific error.
    ///
    /// # Example
    ///
    /// ```rust
    /// use redis_starter_rust::redis::resp;
    ///
    /// let bytes = b"$4\r\nYeah\r\n";
    /// match resp::Value::decode(bytes) {
    ///     Ok(val) => println!("All good: {val}"),
    ///     Err(e) => println!("Oh no, something went wrong: {e}"),
    /// }
    /// ```
    ///
    /// In the above example, we have a BulkString Value in byte-form and we decode it.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let (val, _) = Self::decode_with_len(buf)?;
        Ok(val)
    }

    fn decode_with_len(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() == 0 {
            return Err(DecodeError::EmptyBytes);
        }

        // Get first byte and match type.
        // We already checked that buffer length is greater than 0, so can just unwrap.
        let first_byte = buf.get(0).unwrap().clone();
        match Token::from(first_byte as char) {
            Some(Token::Plus) => {
                let (s, size) = SimpleString::_decode(buf)?;
                Ok((Value::SimpleString(s), size))
            }

            Some(Token::Minus) => {
                let (s, size) = SimpleError::_decode(buf)?;
                Ok((Value::SimpleError(s), size))
            }

            Some(Token::Colon) => {
                let (i, size) = Integer::_decode(buf)?;
                Ok((Value::Integer(i), size))
            }

            Some(Token::Dollar) => {
                let (bs, size) = BulkString::_decode(buf)?;
                Ok((Value::BulkString(bs), size))
            }

            Some(Token::Star) => {
                let (arr, size) = Array::_decode(buf)?;
                Ok((Value::Array(arr), size))
            }

            _ => Err(DecodeError::UnknownType { first_byte }),
        }
    }

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
}

/// Expects input to be in the form of `b"x<string>\r\n..."`, where x is the type of the RESP.
///
/// # Returns
///
/// - `Ok((String, usize))` if no decoding errors. The `usize` represents total bytes read.
/// - `DecodeError::...` if there were some decoding errors, see the enum variants in order to
///     understand what is the specific error.
fn decode_to_string(bytes: &[u8]) -> Result<(String, usize), DecodeError> {
    if let Some((b, size)) = read_until_crlf(bytes) {
        let s = String::from_utf8(b[1..].into())?;
        return Ok((s, size));
    }

    Err(DecodeError::InvalidFormat)
}

/// Expects input to be in the form of `b"x<i64>\r\n..."`, where x is the type of the RESP.
///
/// # Returns
///
/// - `Ok((i64, usize))` if no decoding errors. The `usize` represents total bytes read.
/// - `DecodeError::...` if there were some decoding errors, see the enum variants in order to
///     understand what is the specific error.
fn decode_to_i64(bytes: &[u8]) -> Result<(i64, usize), DecodeError> {
    let (s, size) = decode_to_string(bytes)?;

    Ok((s.parse::<i64>()?, size))
}

/// Read until the first CRLF.
///
/// # Returns
///
/// - `Some((&[u8], usize))` if there is a CRLF. The tuple represents the part of the
///     buffer read and total bytes read.
/// - `None` if there are no CRLFs in the bytes.
fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    return None;
}

#[cfg(test)]
mod util_test {
    use super::*;

    #[test]
    fn read_until_crlf_ok() {
        match read_until_crlf(b"$4\r\nOK22\r\n") {
            Some((bytes, len)) => {
                assert_eq!(bytes, b"$4");
                assert_eq!(len, 4);
            }
            None => panic!("Unexpected read until crlf error"),
        }
    }
}

#[cfg(test)]
mod decoder_test {
    use super::*;

    #[test]
    fn decode_simple_string() {
        let resp = Value::decode(b"+OK\r\n").expect("Decode simple string unexpected error");
        match resp {
            Value::SimpleString(s) => assert_eq!(s.as_str(), "OK"),
            any => panic!("Wrong type for decode simple string: {:?}", any),
        }
    }

    #[test]
    fn decode_simple_error() {
        let resp =
            Value::decode(b"-ERR something\r\n").expect("Decode simple error unexpected error");
        match resp {
            Value::SimpleError(s) => assert_eq!(s.as_str(), "ERR something"),
            any => panic!("Wrong type for decode simple error: {:?}", any),
        }
    }

    #[test]
    fn decode_integer() {
        let resp = Value::decode(b":123\r\n").expect("Decode integer unexpected error");
        match resp {
            Value::Integer(i) => assert_eq!(i.as_int(), 123),
            any => panic!("Wrong type for decode integer: {:?}", any),
        }
    }

    #[test]
    fn decode_bulk_string() {
        let resp = Value::decode(b"$4\r\nHell\r\n").expect("Decode bulk string unexpected error");
        match resp {
            Value::BulkString(bs) => assert_eq!(bs.as_bytes(), Some("Hell".as_bytes())),
            any => panic!("Wrong type for decode bulk string: {:?}", any),
        }
    }

    #[test]
    fn decode_bulk_string_mismatch_len() {
        let err = Value::decode(b"$3\r\nHell\r\n").expect_err("Decode bulk string no error");
        match err {
            DecodeError::LenMismatch { .. } => (),
            any => panic!("Wrong error for decode bulk string: {:?}", any),
        }
    }

    #[test]
    fn decode_bulk_string_parse_len_error() {
        let err = Value::decode(b"$Liberty\r\n").expect_err("Decode bulk string no error");
        match err {
            DecodeError::ParseInt(_) => (),
            any => panic!("Wrong error for decode bulk string: {:?}", any),
        }
    }

    #[test]
    fn decode_array() {
        let resp = Value::decode(b"*2\r\n:12\r\n+Yea\r\n").expect("Decode array unexpected error");
        match resp {
            Value::Array(arr) => {
                let mut iter = arr.values().unwrap().iter();
                assert_eq!(iter.next().unwrap().integer().unwrap().as_int(), 12);
                assert_eq!(
                    iter.next().unwrap().simple_string().unwrap().as_str(),
                    "Yea"
                );
            }
            _ => panic!("Wrong type for decode array"),
        }
    }

    #[test]
    fn decode_array_nested() {
        let resp = Value::decode(
            b"*2\r\n*3\r\n:12\r\n+Yea\r\n-Oopsie\r\n*2\r\n$5\r\nHello\r\n$4\r\nGGWP\r\n",
        )
        .expect("Decode array unexpected error");
        match resp {
            Value::Array(arr) => {
                let first_values = arr
                    .values()
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .array()
                    .unwrap()
                    .values()
                    .unwrap();
                assert_eq!(first_values.get(0).unwrap().integer().unwrap().as_int(), 12);
                assert_eq!(
                    first_values
                        .get(1)
                        .unwrap()
                        .simple_string()
                        .unwrap()
                        .as_str(),
                    "Yea"
                );
                assert_eq!(
                    first_values
                        .get(2)
                        .unwrap()
                        .simple_error()
                        .unwrap()
                        .as_str(),
                    "Oopsie"
                );

                let second_values = arr
                    .values()
                    .unwrap()
                    .get(1)
                    .unwrap()
                    .array()
                    .unwrap()
                    .values()
                    .unwrap();
                assert_eq!(
                    second_values
                        .get(0)
                        .unwrap()
                        .bulk_string()
                        .unwrap()
                        .as_bytes(),
                    Some("Hello".as_bytes())
                );
                assert_eq!(
                    second_values
                        .get(1)
                        .unwrap()
                        .bulk_string()
                        .unwrap()
                        .as_bytes(),
                    Some("GGWP".as_bytes())
                );
            }
            _ => panic!("Wrong type for decode array"),
        }
    }
}
