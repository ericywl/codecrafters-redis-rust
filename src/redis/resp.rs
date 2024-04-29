use std::{fmt::Display, io, num::ParseIntError, string::FromUtf8Error};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EncodeError {
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
    Star = b'*',
    Dollar = b'$',
    Plus = b'+',
    Minus = b'-',
    Colon = b':',
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

impl Into<String> for &SimpleString {
    fn into(self) -> String {
        self.s.clone()
    }
}

impl Encoder for SimpleString {
    /// Returns string formatted as `b"+<string>\r\n"`.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        write!(buf, "{}{}\r\n", Token::Plus, self.s)?;
        Ok(())
    }
}

impl Decoder for SimpleString {
    /// Expects input to be in the form of `b"+<string>\r\n..."`.
    ///
    /// Returns string and total bytes consumed.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        let (s, size) = decode_to_string(buf)?;
        Ok((s.into(), size))
    }
}

impl SimpleString {
    /// Returns SimpleString as string.
    pub fn as_str(&self) -> &str {
        &self.s
    }
}

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

impl Into<String> for &SimpleError {
    fn into(self) -> String {
        self.s.clone()
    }
}

impl Encoder for SimpleError {
    /// Returns SimpleError formatted as `b"-<string>\r\n`.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        write!(buf, "{}{}\r\n", Token::Minus, self.s)?;
        Ok(())
    }
}

impl Decoder for SimpleError {
    /// Expects input to be in the form of `b"-<string>\r\n..."`.
    ///
    /// Returns SimpleError and total bytes consumed.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        let (s, size) = decode_to_string(buf)?;
        Ok((s.into(), size))
    }
}

impl SimpleError {
    /// Returns SimpleError as string.
    pub fn as_str(&self) -> &str {
        &self.s
    }
}

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

impl Encoder for Integer {
    /// Returns Integer formatted as `b":<integer>\r\n"`.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        write!(buf, "{}{}\r\n", Token::Colon, self.i)?;
        Ok(())
    }
}

impl Decoder for Integer {
    /// Expects input to be in the form of `b":<integer>\r\n..."`.
    ///
    /// Returns Integer and total bytes consumed.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        let (i, size) = decode_to_i64(buf)?;
        Ok((i.into(), size))
    }
}

impl Integer {
    /// Returns Integer as int64.
    pub fn as_int(&self) -> i64 {
        self.i
    }
}

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

impl Encoder for BulkString {
    /// Returns BulkString formatted as `b"$<len>\r\n<data>\r\n"`
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        if self.is_null {
            write!(buf, "{}-1\r\n", Token::Dollar)?;
            return Ok(());
        }

        write!(buf, "{}{}\r\n", Token::Dollar, self.bytes.len())?;
        buf.write_all(&self.bytes)?;
        write!(buf, "\r\n")?;
        Ok(())
    }
}

impl Decoder for BulkString {
    /// Expects input to be in the form of `b"$<len>\r\n<data>\r\n..."`.
    ///
    /// Returns BulkString and total bytes consumed.
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
    pub fn null() -> Self {
        Self {
            is_null: true,
            bytes: vec![],
        }
    }

    /// Returns if BulkString is null.
    pub fn is_null(&self) -> bool {
        return self.is_null;
    }

    /// Returns BulkString as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns BulkString as string if it can be encoded into a string.
    /// Otherwise returns None.
    pub fn as_str(&self) -> Option<String> {
        match String::from_utf8(self.bytes.to_vec()) {
            Ok(s) => Some(s),
            Err(_) => None,
        }
    }
}

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

    /// Returns if Array is null.
    pub fn is_null(&self) -> bool {
        self.is_null
    }

    /// Returns list of Values contained in the Array.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

impl Encoder for Array {
    /// Returns Array formatted as`b"$<size>\r\n<element_1>\r\n<element2>\r\n"`.
    fn _encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        if self.is_null {
            write!(buf, "{}-1\r\n", Token::Star)?;
            return Ok(());
        }

        write!(buf, "{}{}\r\n", Token::Star, self.values.len())?;
        for val in &self.values {
            val._encode(buf)?;
        }

        Ok(())
    }
}

impl Decoder for Array {
    /// Expects input to be in the form of `b"$<size>\r\n<element_1>\r\n<element2>\r\n..."`.
    ///
    /// Returns Array and total bytes consumed.
    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Sized,
    {
        // Consume `b"$<size>\r\n"`
        let (arr_size, mut bytes_consumed) = decode_to_i64(buf)?;
        if arr_size < 0 {
            return Ok((Array::null(), bytes_consumed));
        }

        let mut values = vec![];
        for _ in 0..arr_size {
            let (val, len) = Value::_decode(&buf[bytes_consumed..])?;
            values.push(val);
            bytes_consumed += len;
        }

        Ok((Array::from(values), bytes_consumed))
    }
}

#[derive(Clone, Debug)]
#[enum_delegate::implement(Encoder)]
pub enum Value {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(Integer),
    BulkString(BulkString),
    Array(Array),
}

impl Value {
    pub fn encode(&self, buf: &mut impl io::Write) -> Result<(), EncodeError> {
        self._encode(buf)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let (val, _) = Self::_decode(buf)?;
        Ok(val)
    }

    fn _decode(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() == 0 {
            return Err(DecodeError::EmptyBytes);
        }

        // Get first byte and match type
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
/// Returns string and total bytes consumed.
fn decode_to_string(bytes: &[u8]) -> Result<(String, usize), DecodeError> {
    if let Some((b, size)) = read_until_crlf(bytes) {
        let s = String::from_utf8(b[1..].into())?;
        return Ok((s, size));
    }

    Err(DecodeError::InvalidFormat)
}

/// Expects input to be in the form of `b"x<i64>\r\n..."`, where x is the type of the RESP.
///
/// Returns string and total bytes consumed.
fn decode_to_i64(bytes: &[u8]) -> Result<(i64, usize), DecodeError> {
    let (s, size) = decode_to_string(bytes)?;

    Ok((s.parse::<i64>()?, size))
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
            Value::BulkString(bs) => assert_eq!(bs.as_bytes(), b"Hell"),
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
                let mut iter = arr.values().iter();
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
                let first_values = arr.values().get(0).unwrap().array().unwrap().values();
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

                let second_values = arr.values().get(1).unwrap().array().unwrap().values();
                assert_eq!(
                    second_values
                        .get(0)
                        .unwrap()
                        .bulk_string()
                        .unwrap()
                        .as_bytes(),
                    "Hello".as_bytes()
                );
                assert_eq!(
                    second_values
                        .get(1)
                        .unwrap()
                        .bulk_string()
                        .unwrap()
                        .as_bytes(),
                    "GGWP".as_bytes()
                );
            }
            _ => panic!("Wrong type for decode array"),
        }
    }
}
