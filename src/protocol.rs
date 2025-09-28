use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use std::str;

use crate::{EntityId, EntityType, FieldType, Value, Timestamp, FieldSchema, AdjustBehavior, PageOpts, NotifyConfig, PushCondition, EntitySchema, Single, Complete, PageResult, Snapshot};
use crate::data::StorageScope;

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const CRLF: &[u8] = b"\r\n";

/// Trait for types that can encode themselves to RESP format
pub trait RespEncode {
    fn encode_to(&self, out: &mut Vec<u8>);
    
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_to(&mut out);
        out
    }
}

/// Trait for types that can decode themselves from RESP frames
pub trait RespDecode: Sized {
    fn decode_from(bytes: &Bytes) -> Result<Self>;
}

#[derive(Debug, Clone)]
pub struct QuspCommand {
	pub name: Bytes,
	pub args: Vec<Bytes>,
}

impl QuspCommand {
	pub fn new<N: AsRef<[u8]>>(name: N, args: Vec<Bytes>) -> Self {
		Self {
			name: Bytes::copy_from_slice(name.as_ref()),
			args,
		}
	}

	pub fn uppercase_name(&self) -> Result<String> {
		std::str::from_utf8(self.name.as_ref())
			.map(|s| s.to_ascii_uppercase())
			.map_err(|_| anyhow!("command name was not valid UTF-8"))
	}
}

#[derive(Debug, Clone)]
pub enum QuspResponse {
	Simple(Bytes),
	Bulk(Bytes),
	Integer(i64),
	Null,
	Array(Vec<QuspResponse>),
	Error(String),
}

#[derive(Debug, Clone)]
pub enum QuspFrame {
	Command(QuspCommand),
	Response(QuspResponse),
}

enum ParseStatus<T> {
	Complete(T),
	Incomplete,
}

fn read_line_end(input: &[u8], start: usize) -> ParseStatus<usize> {
	let mut idx = start;
	while idx + 1 < input.len() {
		if input[idx] == b'\r' && input[idx + 1] == b'\n' {
			return ParseStatus::Complete(idx);
		}
		idx += 1;
	}
	ParseStatus::Incomplete
}

fn parse_number_line(input: &[u8], start: usize) -> Result<ParseStatus<(i64, usize)>> {
	match read_line_end(input, start) {
		ParseStatus::Incomplete => Ok(ParseStatus::Incomplete),
		ParseStatus::Complete(end) => {
			if start == end {
				return Err(anyhow!("empty integer"));
			}

			let mut idx = start;
			let mut negative = false;
			if input[idx] == b'-' {
				negative = true;
				idx += 1;
			}

			if idx == end {
				return Err(anyhow!("invalid integer"));
			}

			let mut value: i64 = 0;
			while idx < end {
				let byte = input[idx];
				if !(b'0'..=b'9').contains(&byte) {
					return Err(anyhow!("invalid digit in integer"));
				}
				value = value
					.checked_mul(10)
					.ok_or_else(|| anyhow!("integer overflow"))?;
				value = value
					.checked_add((byte - b'0') as i64)
					.ok_or_else(|| anyhow!("integer overflow"))?;
				idx += 1;
			}

			if negative {
				value = -value;
			}

			Ok(ParseStatus::Complete((value, end + 2)))
		}
	}
}

fn skip_frame(input: &[u8], start: usize) -> Result<ParseStatus<usize>> {
	if start >= input.len() {
		return Ok(ParseStatus::Incomplete);
	}

	match input[start] {
		b'+' | b'-' | b':' => Ok(match read_line_end(input, start + 1) {
			ParseStatus::Complete(end) => ParseStatus::Complete(end + 2),
			ParseStatus::Incomplete => ParseStatus::Incomplete,
		}),
		b'$' => match parse_number_line(input, start + 1)? {
			ParseStatus::Incomplete => Ok(ParseStatus::Incomplete),
			ParseStatus::Complete((len, mut idx)) => {
				if len < -1 {
					return Err(anyhow!("invalid bulk length"));
				}
				if len == -1 {
					return Ok(ParseStatus::Complete(idx));
				}
				let len = len as usize;
				if idx + len + 2 > input.len() {
					return Ok(ParseStatus::Incomplete);
				}
				idx += len + 2;
				Ok(ParseStatus::Complete(idx))
			}
		},
		b'*' => match parse_number_line(input, start + 1)? {
			ParseStatus::Incomplete => Ok(ParseStatus::Incomplete),
			ParseStatus::Complete((count, mut idx)) => {
				if count < -1 {
					return Err(anyhow!("invalid array length"));
				}
				if count == -1 {
					return Ok(ParseStatus::Complete(idx));
				}
				let count = count as usize;
				for _ in 0..count {
					match skip_frame(input, idx)? {
						ParseStatus::Incomplete => return Ok(ParseStatus::Incomplete),
						ParseStatus::Complete(next) => idx = next,
					}
				}
				Ok(ParseStatus::Complete(idx))
			}
		},
		_ => Err(anyhow!("unsupported RESP type")),
	}
}

fn try_parse_message_length(input: &[u8]) -> Result<Option<usize>> {
	match skip_frame(input, 0)? {
		ParseStatus::Incomplete => Ok(None),
		ParseStatus::Complete(len) => {
			if len > MAX_MESSAGE_SIZE {
				return Err(anyhow!("RESP frame too large: {} bytes", len));
			}
			Ok(Some(len))
		}
	}
}

#[derive(Debug, Clone)]
enum RespFrame {
	Simple(Bytes),
	Error(Bytes),
	Integer(i64),
	Bulk(Bytes),
	Null,
	Array(Vec<RespFrame>),
}

fn parse_frame(bytes: &Bytes, start: usize) -> Result<(RespFrame, usize)> {
	if start >= bytes.len() {
		return Err(anyhow!("unexpected end of RESP frame"));
	}

	match bytes[start] {
		b'+' => match read_line_end(bytes.as_ref(), start + 1) {
			ParseStatus::Complete(end) => {
				let slice = bytes.slice((start + 1)..end);
				Ok((RespFrame::Simple(slice), end + 2))
			}
			ParseStatus::Incomplete => Err(anyhow!("unterminated simple string")),
		},
		b'-' => match read_line_end(bytes.as_ref(), start + 1) {
			ParseStatus::Complete(end) => {
				let slice = bytes.slice((start + 1)..end);
				Ok((RespFrame::Error(slice), end + 2))
			}
			ParseStatus::Incomplete => Err(anyhow!("unterminated error string")),
		},
		b':' => match parse_number_line(bytes.as_ref(), start + 1)? {
			ParseStatus::Complete((value, idx)) => Ok((RespFrame::Integer(value), idx)),
			ParseStatus::Incomplete => Err(anyhow!("unterminated integer")),
		},
		b'$' => match parse_number_line(bytes.as_ref(), start + 1)? {
			ParseStatus::Complete((len, mut idx)) => {
				if len < -1 {
					return Err(anyhow!("invalid bulk length"));
				}
				if len == -1 {
					return Ok((RespFrame::Null, idx));
				}
				let len = len as usize;
				if idx + len + 2 > bytes.len() {
					return Err(anyhow!("truncated bulk string"));
				}
				let data = bytes.slice(idx..idx + len);
				idx += len;
				if &bytes[idx..idx + 2] != CRLF {
					return Err(anyhow!("bulk string missing CRLF terminator"));
				}
				idx += 2;
				Ok((RespFrame::Bulk(data), idx))
			}
			ParseStatus::Incomplete => Err(anyhow!("unterminated bulk length")),
		},
		b'*' => match parse_number_line(bytes.as_ref(), start + 1)? {
			ParseStatus::Complete((len, mut idx)) => {
				if len < -1 {
					return Err(anyhow!("invalid array length"));
				}
				if len == -1 {
					return Ok((RespFrame::Null, idx));
				}
				let len = len as usize;
				let mut items = Vec::with_capacity(len);
				for _ in 0..len {
					let (frame, next_idx) = parse_frame(bytes, idx)?;
					items.push(frame);
					idx = next_idx;
				}
				Ok((RespFrame::Array(items), idx))
			}
			ParseStatus::Incomplete => Err(anyhow!("unterminated array length")),
		},
		_ => Err(anyhow!("unsupported RESP type")),
	}
}

fn parse_root_frame(bytes: &Bytes) -> Result<RespFrame> {
	let (frame, consumed) = parse_frame(bytes, 0)?;
	if consumed != bytes.len() {
		return Err(anyhow!("extra bytes after QUSP frame"));
	}
	Ok(frame)
}

fn frame_into_bytes(frame: RespFrame) -> Result<Bytes> {
	match frame {
		RespFrame::Bulk(bytes) | RespFrame::Simple(bytes) => Ok(bytes),
		_ => Err(anyhow!("expected QUSP string frame")),
	}
}

fn frame_to_command(frame: RespFrame) -> Result<QuspCommand> {
	let items = match frame {
		RespFrame::Array(items) => items,
		RespFrame::Null => Vec::new(),
		_ => return Err(anyhow!("expected QUSP array frame for command")),
	};

	if items.is_empty() {
		return Err(anyhow!("QUSP command missing name"));
	}

	let mut iter = items.into_iter();
	let name_frame = iter
		.next()
		.ok_or_else(|| anyhow!("QUSP command missing name element"))?;
	let name = frame_into_bytes(name_frame)?;

	let mut args = Vec::new();
	for item in iter {
		args.push(frame_into_bytes(item)?);
	}

	Ok(QuspCommand { name, args })
}

fn frame_to_response(frame: RespFrame) -> Result<QuspResponse> {
	Ok(match frame {
		RespFrame::Simple(bytes) => QuspResponse::Simple(bytes),
		RespFrame::Bulk(bytes) => QuspResponse::Bulk(bytes),
		RespFrame::Integer(value) => QuspResponse::Integer(value),
		RespFrame::Null => QuspResponse::Null,
		RespFrame::Error(bytes) => QuspResponse::Error(
			String::from_utf8_lossy(bytes.as_ref()).into_owned(),
		),
		RespFrame::Array(items) => {
			let mut converted = Vec::with_capacity(items.len());
			for item in items {
				converted.push(frame_to_response(item)?);
			}
			QuspResponse::Array(converted)
		}
	})
}

fn encode_decimal(mut value: u64, buf: &mut [u8; 20]) -> &[u8] {
	if value == 0 {
		buf[buf.len() - 1] = b'0';
		return &buf[buf.len() - 1..];
	}

	let mut idx = buf.len();
	while value > 0 {
		idx -= 1;
		buf[idx] = b'0' + (value % 10) as u8;
		value /= 10;
	}

	&buf[idx..]
}

fn write_array_header(out: &mut Vec<u8>, len: usize) {
	out.push(b'*');
	let mut buf = [0u8; 20];
	out.extend_from_slice(encode_decimal(len as u64, &mut buf));
	out.extend_from_slice(CRLF);
}

fn write_bulk(out: &mut Vec<u8>, data: &[u8]) {
	out.push(b'$');
	let mut buf = [0u8; 20];
	out.extend_from_slice(encode_decimal(data.len() as u64, &mut buf));
	out.extend_from_slice(CRLF);
	out.extend_from_slice(data);
	out.extend_from_slice(CRLF);
}

fn write_simple(out: &mut Vec<u8>, data: &[u8]) {
	out.push(b'+');
	out.extend_from_slice(data);
	out.extend_from_slice(CRLF);
}

fn write_error(out: &mut Vec<u8>, data: &[u8]) {
	out.push(b'-');
	out.extend_from_slice(data);
	out.extend_from_slice(CRLF);
}

fn write_integer(out: &mut Vec<u8>, value: i64) {
    out.push(b':');
    out.extend_from_slice(value.to_string().as_bytes());
    out.extend_from_slice(CRLF);
}

fn write_null(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1");
    out.extend_from_slice(CRLF);
}

fn write_response(out: &mut Vec<u8>, response: &QuspResponse) {
    match response {
        QuspResponse::Simple(bytes) => write_simple(out, bytes.as_ref()),
        QuspResponse::Bulk(bytes) => write_bulk(out, bytes.as_ref()),
        QuspResponse::Integer(i) => write_integer(out, *i),
        QuspResponse::Null => write_null(out),
        QuspResponse::Array(arr) => {
            write_array_header(out, arr.len());
            for r in arr {
                write_response(out, r);
            }
        }
        QuspResponse::Error(s) => write_error(out, s.as_bytes()),
    }
}

pub fn encode_command(command: &QuspCommand) -> Vec<u8> {
	let mut out = Vec::with_capacity(128);
	write_array_header(&mut out, 1 + command.args.len());
	write_bulk(&mut out, command.name.as_ref());
	for arg in &command.args {
		write_bulk(&mut out, arg.as_ref());
	}
	out
}

pub fn encode_simple_string(message: &str) -> Vec<u8> {
	let mut out = Vec::with_capacity(message.len() + 4);
	write_simple(&mut out, message.as_bytes());
	out
}

pub fn encode_ok() -> Vec<u8> {
	encode_simple_string("OK")
}

pub fn encode_bulk_bytes(data: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(data.len() + 8);
	write_bulk(&mut out, data);
	out
}

pub fn encode_integer(value: i64) -> Vec<u8> {
	let mut out = Vec::with_capacity(32);
	out.push(b':');
	out.extend_from_slice(value.to_string().as_bytes());
	out.extend_from_slice(CRLF);
	out
}

pub fn encode_null() -> Vec<u8> {
	let mut out = Vec::with_capacity(5);
	out.extend_from_slice(b"$-1");
	out.extend_from_slice(CRLF);
	out
}

pub fn encode_error(message: &str) -> Vec<u8> {
	let mut out = Vec::with_capacity(message.len() + 4);
	write_error(&mut out, message.as_bytes());
	out
}

// Trait implementations for basic types
impl RespEncode for i64 {
    fn encode_to(&self, out: &mut Vec<u8>) {
        write_integer(out, *self);
    }
}

impl RespEncode for String {
    fn encode_to(&self, out: &mut Vec<u8>) {
        write_bulk(out, self.as_bytes());
    }
}

impl RespEncode for &str {
    fn encode_to(&self, out: &mut Vec<u8>) {
        write_bulk(out, self.as_bytes());
    }
}

impl RespEncode for &[u8] {
    fn encode_to(&self, out: &mut Vec<u8>) {
        write_bulk(out, self);
    }
}

impl RespEncode for Bytes {
    fn encode_to(&self, out: &mut Vec<u8>) {
        write_bulk(out, self.as_ref());
    }
}

impl<T: RespEncode> RespEncode for Vec<T> {
    fn encode_to(&self, out: &mut Vec<u8>) {
        write_array_header(out, self.len());
        for item in self {
            item.encode_to(out);
        }
    }
}

impl<T: RespEncode> RespEncode for Option<T> {
    fn encode_to(&self, out: &mut Vec<u8>) {
        match self {
            Some(value) => value.encode_to(out),
            None => write_null(out),
        }
    }
}

impl RespEncode for QuspResponse {
    fn encode_to(&self, out: &mut Vec<u8>) {
        write_response(out, self);
    }
}

// Trait implementations for decoding basic types
impl RespDecode for String {
    fn decode_from(bytes: &Bytes) -> Result<Self> {
        parse_str(bytes).map(|s| s.to_string())
    }
}

impl RespDecode for u32 {
    fn decode_from(bytes: &Bytes) -> Result<Self> {
        parse_u32(bytes)
    }
}

impl RespDecode for u64 {
    fn decode_from(bytes: &Bytes) -> Result<Self> {
        parse_u64(bytes)
    }
}

impl RespDecode for EntityId {
    fn decode_from(bytes: &Bytes) -> Result<Self> {
        parse_entity_id(bytes)
    }
}

impl RespDecode for EntityType {
    fn decode_from(bytes: &Bytes) -> Result<Self> {
        parse_entity_type(bytes)
    }
}

impl RespDecode for FieldType {
    fn decode_from(bytes: &Bytes) -> Result<Self> {
        parse_field_type(bytes)
    }
}

#[derive(Debug)]
pub struct MessageBuffer {
	buffer: BytesMut,
	max_capacity: usize,
}

impl MessageBuffer {
	pub fn new() -> Self {
		Self::with_capacity(64 * 1024)
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			buffer: BytesMut::with_capacity(capacity),
			max_capacity: capacity,
		}
	}

	pub fn add_data(&mut self, data: &[u8]) {
		self.buffer.extend_from_slice(data);
		if self.buffer.capacity() > self.max_capacity * 4 {
			let mut new_buffer = BytesMut::with_capacity(self.max_capacity);
			new_buffer.extend_from_slice(&self.buffer);
			self.buffer = new_buffer;
		}
	}

	pub fn try_decode(&mut self) -> Result<Option<QuspFrame>> {
		match try_parse_message_length(self.buffer.as_ref())? {
			Some(len) => {
				if self.buffer.len() < len {
					return Ok(None);
				}
				let bytes = self.buffer.split_to(len).freeze();
				let frame = parse_root_frame(&bytes)?;
				match frame {
					frame @ RespFrame::Array(_) => {
						let command = frame_to_command(frame)?;
						Ok(Some(QuspFrame::Command(command)))
					}
					other => {
						let response = frame_to_response(other)?;
						Ok(Some(QuspFrame::Response(response)))
					}
				}
			}
			None => Ok(None),
		}
	}
}

impl Default for MessageBuffer {
	fn default() -> Self {
		Self::new()
	}
}

fn parse_str(bytes: &Bytes) -> Result<&str> {
    std::str::from_utf8(bytes.as_ref()).map_err(|e| anyhow!("invalid UTF-8: {}", e))
}

fn parse_u32(bytes: &Bytes) -> Result<u32> {
    let s = parse_str(bytes)?;
    s.parse().map_err(|e| anyhow!("invalid u32: {}", e))
}

fn parse_u64(bytes: &Bytes) -> Result<u64> {
    let s = parse_str(bytes)?;
    s.parse().map_err(|e| anyhow!("invalid u64: {}", e))
}

fn parse_entity_id(bytes: &Bytes) -> Result<EntityId> {
    parse_u64(bytes).map(EntityId)
}

fn parse_entity_type(bytes: &Bytes) -> Result<EntityType> {
    parse_u32(bytes).map(EntityType)
}

fn parse_field_type(bytes: &Bytes) -> Result<FieldType> {
    parse_u64(bytes).map(FieldType)
}

fn parse_timestamp(s: &str) -> Result<Timestamp> {
    let nanos: i64 = s.parse().map_err(|e| anyhow!("invalid timestamp: {}", e))?;
    Ok(crate::nanos_to_timestamp(nanos as u64))
}

// Binary encoding for Value enum using zero-copy deserialization
// Format: [type_tag:u8][length:u64][data...]
pub fn encode_value(value: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    match value {
        Value::String(s) => {
            buf.push(0); // type tag for String
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::Int(i) => {
            buf.push(1); // type tag for Int
            buf.extend_from_slice(&8u64.to_le_bytes()); // length
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            buf.push(2); // type tag for Float
            buf.extend_from_slice(&8u64.to_le_bytes()); // length
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Bool(b) => {
            buf.push(3); // type tag for Bool
            buf.extend_from_slice(&1u64.to_le_bytes()); // length
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Blob(data) => {
            buf.push(4); // type tag for Blob
            buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
            buf.extend_from_slice(data);
        }
        Value::EntityReference(opt_id) => {
            buf.push(5); // type tag for EntityReference
            if let Some(id) = opt_id {
                buf.extend_from_slice(&8u64.to_le_bytes()); // length
                buf.extend_from_slice(&id.0.to_le_bytes());
            } else {
                buf.extend_from_slice(&0u64.to_le_bytes()); // length 0 for None
            }
        }
        Value::EntityList(ids) => {
            buf.push(6); // type tag for EntityList
            buf.extend_from_slice(&((ids.len() * 8) as u64).to_le_bytes()); // length
            for id in ids {
                buf.extend_from_slice(&id.0.to_le_bytes());
            }
        }
        Value::Choice(choice) => {
            buf.push(7); // type tag for Choice
            buf.extend_from_slice(&8u64.to_le_bytes()); // length
            buf.extend_from_slice(&choice.to_le_bytes());
        }
        Value::Timestamp(ts) => {
            buf.push(8); // type tag for Timestamp
            buf.extend_from_slice(&16u64.to_le_bytes()); // length for unix timestamp nanos
            let unix_nanos = ts.unix_timestamp_nanos() as u128;
            buf.extend_from_slice(&unix_nanos.to_le_bytes());
        }
    }
    buf
}

fn decode_value(bytes: &Bytes) -> Result<Value> {
    if bytes.len() < 9 {
        return Err(anyhow!("Value bytes too short"));
    }
    
    let type_tag = bytes[0];
    let len_bytes = &bytes[1..9];
    let length = u64::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    
    if bytes.len() < 9 + length {
        return Err(anyhow!("Value bytes truncated"));
    }
    
    let data = &bytes[9..9 + length];
    
    match type_tag {
        0 => { // String
            let s = std::str::from_utf8(data).map_err(|e| anyhow!("invalid UTF-8 in string value: {}", e))?;
            Ok(Value::String(s.to_string()))
        }
        1 => { // Int
            if data.len() != 8 {
                return Err(anyhow!("invalid int data length"));
            }
            let i = i64::from_le_bytes(data.try_into().unwrap());
            Ok(Value::Int(i))
        }
        2 => { // Float
            if data.len() != 8 {
                return Err(anyhow!("invalid float data length"));
            }
            let f = f64::from_le_bytes(data.try_into().unwrap());
            Ok(Value::Float(f))
        }
        3 => { // Bool
            if data.len() != 1 {
                return Err(anyhow!("invalid bool data length"));
            }
            Ok(Value::Bool(data[0] != 0))
        }
        4 => { // Blob
            Ok(Value::Blob(data.to_vec()))
        }
        5 => { // EntityReference
            if data.len() == 0 {
                Ok(Value::EntityReference(None))
            } else if data.len() == 8 {
                let id = u64::from_le_bytes(data.try_into().unwrap());
                Ok(Value::EntityReference(Some(EntityId(id))))
            } else {
                Err(anyhow!("invalid entity reference data length"))
            }
        }
        6 => { // EntityList
            if data.len() % 8 != 0 {
                return Err(anyhow!("invalid entity list data length"));
            }
            let mut ids = Vec::new();
            for chunk in data.chunks(8) {
                let id = u64::from_le_bytes(chunk.try_into().unwrap());
                ids.push(EntityId(id));
            }
            Ok(Value::EntityList(ids))
        }
        7 => { // Choice
            if data.len() != 8 {
                return Err(anyhow!("invalid choice data length"));
            }
            let choice = i64::from_le_bytes(data.try_into().unwrap());
            Ok(Value::Choice(choice))
        }
        8 => { // Timestamp
            if data.len() != 16 {
                return Err(anyhow!("invalid timestamp data length"));
            }
            let unix_nanos = u128::from_le_bytes(data.try_into().unwrap());
            let ts = crate::nanos_to_timestamp(unix_nanos as u64);
            Ok(Value::Timestamp(ts))
        }
        _ => Err(anyhow!("unknown value type tag: {}", type_tag)),
    }
}

// RESP encoding for FieldSchema - format: [type_name, field_type_id, default_value_encoded, rank, storage_scope]
pub fn encode_field_schema(schema: &FieldSchema) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    
    match schema {
        FieldSchema::String { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"String");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            write_bulk(&mut out, default_value.as_bytes());
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        FieldSchema::Int { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Int");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            write_bulk(&mut out, &default_value.to_le_bytes());
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        FieldSchema::Float { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Float");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            write_bulk(&mut out, &default_value.to_le_bytes());
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        FieldSchema::Bool { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Bool");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            write_bulk(&mut out, &[if *default_value { 1 } else { 0 }]);
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        FieldSchema::Blob { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Blob");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            write_bulk(&mut out, default_value);
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        FieldSchema::EntityReference { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"EntityReference");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            if let Some(entity_id) = default_value {
                write_bulk(&mut out, &entity_id.0.to_le_bytes());
            } else {
                write_null(&mut out);
            }
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        FieldSchema::EntityList { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"EntityList");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            // Encode entity list as array of entity IDs
            let mut list_data = Vec::new();
            write_array_header(&mut list_data, default_value.len());
            for entity_id in default_value {
                write_bulk(&mut list_data, &entity_id.0.to_le_bytes());
            }
            write_bulk(&mut out, &list_data);
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope } => {
            write_array_header(&mut out, 6); // 6 elements for Choice (includes choices)
            write_bulk(&mut out, b"Choice");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            write_bulk(&mut out, &default_value.to_le_bytes());
            write_integer(&mut out, *rank);
            // Encode choices as array of strings
            let mut choices_data = Vec::new();
            write_array_header(&mut choices_data, choices.len());
            for choice in choices {
                write_bulk(&mut choices_data, choice.as_bytes());
            }
            write_bulk(&mut out, &choices_data);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        FieldSchema::Timestamp { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Timestamp");
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            let unix_nanos = default_value.unix_timestamp_nanos() as u128;
            write_bulk(&mut out, &unix_nanos.to_le_bytes());
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
    }
    
    Ok(out)
}

fn decode_field_schema(bytes: &Bytes) -> Result<FieldSchema> {
    // Parse the RESP array
    let frame = parse_root_frame(bytes)?;
    let items = match frame {
        RespFrame::Array(items) => items,
        _ => return Err(anyhow!("Expected array for FieldSchema")),
    };
    
    if items.len() < 5 {
        return Err(anyhow!("FieldSchema array too short"));
    }
    
    let type_name = match &items[0] {
        RespFrame::Bulk(bytes) => std::str::from_utf8(bytes)?,
        _ => return Err(anyhow!("Expected string for FieldSchema type name")),
    };
    
    let field_type = match &items[1] {
        RespFrame::Bulk(bytes) => {
            if bytes.len() != 8 {
                return Err(anyhow!("Invalid field type bytes"));
            }
            FieldType(u64::from_le_bytes(bytes.as_ref().try_into().unwrap()))
        }
        _ => return Err(anyhow!("Expected bytes for field type")),
    };
    
    let rank = match &items[3] {
        RespFrame::Integer(i) => *i,
        _ => return Err(anyhow!("Expected integer for rank")),
    };
    
    let storage_scope = match &items[4] {
        RespFrame::Bulk(bytes) => {
            match std::str::from_utf8(bytes)? {
                "Runtime" => StorageScope::Runtime,
                "Configuration" => StorageScope::Configuration,
                s => return Err(anyhow!("Unknown storage scope: {}", s)),
            }
        }
        _ => return Err(anyhow!("Expected string for storage scope")),
    };
    
    match type_name {
        "String" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => String::from_utf8(bytes.to_vec())?,
                _ => return Err(anyhow!("Expected string for default value")),
            };
            Ok(FieldSchema::String { field_type, default_value, rank, storage_scope })
        }
        "Int" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid int bytes"));
                    }
                    i64::from_le_bytes(bytes.as_ref().try_into().unwrap())
                }
                _ => return Err(anyhow!("Expected bytes for int default value")),
            };
            Ok(FieldSchema::Int { field_type, default_value, rank, storage_scope })
        }
        "Float" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid float bytes"));
                    }
                    f64::from_le_bytes(bytes.as_ref().try_into().unwrap())
                }
                _ => return Err(anyhow!("Expected bytes for float default value")),
            };
            Ok(FieldSchema::Float { field_type, default_value, rank, storage_scope })
        }
        "Bool" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 1 {
                        return Err(anyhow!("Invalid bool bytes"));
                    }
                    bytes[0] != 0
                }
                _ => return Err(anyhow!("Expected bytes for bool default value")),
            };
            Ok(FieldSchema::Bool { field_type, default_value, rank, storage_scope })
        }
        "Blob" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => bytes.to_vec(),
                _ => return Err(anyhow!("Expected bytes for blob default value")),
            };
            Ok(FieldSchema::Blob { field_type, default_value, rank, storage_scope })
        }
        "EntityReference" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid entity reference bytes"));
                    }
                    Some(EntityId(u64::from_le_bytes(bytes.as_ref().try_into().unwrap())))
                }
                RespFrame::Null => None,
                _ => return Err(anyhow!("Expected bytes or null for entity reference default value")),
            };
            Ok(FieldSchema::EntityReference { field_type, default_value, rank, storage_scope })
        }
        "EntityList" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(list_bytes) => {
                    // Parse the nested RESP array
                    let list_frame = parse_root_frame(&Bytes::copy_from_slice(list_bytes))?;
                    match list_frame {
                        RespFrame::Array(list_items) => {
                            let mut entity_ids = Vec::new();
                            for item in list_items {
                                match item {
                                    RespFrame::Bulk(bytes) => {
                                        if bytes.len() != 8 {
                                            return Err(anyhow!("Invalid entity ID bytes"));
                                        }
                                        entity_ids.push(EntityId(u64::from_le_bytes(bytes.as_ref().try_into().unwrap())));
                                    }
                                    _ => return Err(anyhow!("Expected bytes for entity ID")),
                                }
                            }
                            entity_ids
                        }
                        _ => return Err(anyhow!("Expected array for entity list")),
                    }
                }
                _ => return Err(anyhow!("Expected bytes for entity list default value")),
            };
            Ok(FieldSchema::EntityList { field_type, default_value, rank, storage_scope })
        }
        "Choice" => {
            if items.len() != 6 {
                return Err(anyhow!("Choice FieldSchema expects 6 elements"));
            }
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid choice bytes"));
                    }
                    i64::from_le_bytes(bytes.as_ref().try_into().unwrap())
                }
                _ => return Err(anyhow!("Expected bytes for choice default value")),
            };
            let choices = match &items[4] {
                RespFrame::Bulk(choices_bytes) => {
                    // Parse the nested RESP array
                    let choices_frame = parse_root_frame(&Bytes::copy_from_slice(choices_bytes))?;
                    match choices_frame {
                        RespFrame::Array(choice_items) => {
                            let mut choice_strings = Vec::new();
                            for item in choice_items {
                                match item {
                                    RespFrame::Bulk(bytes) => {
                                        choice_strings.push(String::from_utf8(bytes.to_vec())?);
                                    }
                                    _ => return Err(anyhow!("Expected bytes for choice string")),
                                }
                            }
                            choice_strings
                        }
                        _ => return Err(anyhow!("Expected array for choices")),
                    }
                }
                _ => return Err(anyhow!("Expected bytes for choices")),
            };
            Ok(FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope })
        }
        "Timestamp" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 16 {
                        return Err(anyhow!("Invalid timestamp bytes"));
                    }
                    let unix_nanos = u128::from_le_bytes(bytes.as_ref().try_into().unwrap());
                    crate::nanos_to_timestamp(unix_nanos as u64)
                }
                _ => return Err(anyhow!("Expected bytes for timestamp default value")),
            };
            Ok(FieldSchema::Timestamp { field_type, default_value, rank, storage_scope })
        }
        _ => Err(anyhow!("Unknown FieldSchema type: {}", type_name)),
    }
}

// Binary encoding for PageOpts
pub fn encode_page_opts(opts: &PageOpts) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(opts.limit as u64).to_le_bytes());
    if let Some(cursor) = opts.cursor {
        buf.push(1); // has cursor
        buf.extend_from_slice(&(cursor as u64).to_le_bytes());
    } else {
        buf.push(0); // no cursor
    }
    buf
}

fn decode_page_opts(bytes: &Bytes) -> Result<PageOpts> {
    if bytes.len() < 9 {
        return Err(anyhow!("PageOpts bytes too short"));
    }
    let limit = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
    let has_cursor = bytes[8] != 0;
    let cursor = if has_cursor {
        if bytes.len() < 17 {
            return Err(anyhow!("PageOpts cursor bytes missing"));
        }
        Some(u64::from_le_bytes(bytes[9..17].try_into().unwrap()) as usize)
    } else {
        None
    };
    Ok(PageOpts::new(limit, cursor))
}

// RESP encoding for NotifyConfig - format depends on variant
pub fn encode_notify_config(config: &NotifyConfig) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    
    match config {
        NotifyConfig::EntityId { entity_id, field_type, trigger_on_change, context } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"EntityId");
            write_bulk(&mut out, &entity_id.0.to_le_bytes());
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            write_bulk(&mut out, &[if *trigger_on_change { 1 } else { 0 }]);
            
            // Encode context as nested array
            let mut context_data = Vec::new();
            write_array_header(&mut context_data, context.len());
            for field_path in context {
                let mut path_data = Vec::new();
                write_array_header(&mut path_data, field_path.len());
                for field_type in field_path {
                    write_bulk(&mut path_data, &field_type.0.to_le_bytes());
                }
                context_data.extend_from_slice(&path_data);
            }
            write_bulk(&mut out, &context_data);
        }
        NotifyConfig::EntityType { entity_type, field_type, trigger_on_change, context } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"EntityType");
            write_bulk(&mut out, &entity_type.0.to_le_bytes());
            write_bulk(&mut out, &field_type.0.to_le_bytes());
            write_bulk(&mut out, &[if *trigger_on_change { 1 } else { 0 }]);
            
            // Encode context as nested array
            let mut context_data = Vec::new();
            write_array_header(&mut context_data, context.len());
            for field_path in context {
                let mut path_data = Vec::new();
                write_array_header(&mut path_data, field_path.len());
                for field_type in field_path {
                    write_bulk(&mut path_data, &field_type.0.to_le_bytes());
                }
                context_data.extend_from_slice(&path_data);
            }
            write_bulk(&mut out, &context_data);
        }
    }
    
    Ok(out)
}

fn decode_notify_config(bytes: &Bytes) -> Result<NotifyConfig> {
    let frame = parse_root_frame(bytes)?;
    let items = match frame {
        RespFrame::Array(items) => items,
        _ => return Err(anyhow!("Expected array for NotifyConfig")),
    };
    
    if items.len() != 5 {
        return Err(anyhow!("NotifyConfig array expects 5 elements"));
    }
    
    let variant_name = match &items[0] {
        RespFrame::Bulk(bytes) => std::str::from_utf8(bytes)?,
        _ => return Err(anyhow!("Expected string for NotifyConfig variant")),
    };
    
    let trigger_on_change = match &items[3] {
        RespFrame::Bulk(bytes) => {
            if bytes.len() != 1 {
                return Err(anyhow!("Invalid trigger_on_change bytes"));
            }
            bytes[0] != 0
        }
        _ => return Err(anyhow!("Expected bytes for trigger_on_change")),
    };
    
    let context = match &items[4] {
        RespFrame::Bulk(context_bytes) => {
            let context_frame = parse_root_frame(&Bytes::copy_from_slice(context_bytes))?;
            match context_frame {
                RespFrame::Array(context_items) => {
                    let mut context_vec = Vec::new();
                    for item in context_items {
                        match item {
                            RespFrame::Array(path_items) => {
                                let mut field_path = Vec::new();
                                for path_item in path_items {
                                    match path_item {
                                        RespFrame::Bulk(bytes) => {
                                            if bytes.len() != 8 {
                                                return Err(anyhow!("Invalid field type bytes"));
                                            }
                                            field_path.push(FieldType(u64::from_le_bytes(bytes.as_ref().try_into().unwrap())));
                                        }
                                        _ => return Err(anyhow!("Expected bytes for field type")),
                                    }
                                }
                                context_vec.push(field_path);
                            }
                            _ => return Err(anyhow!("Expected array for field path")),
                        }
                    }
                    context_vec
                }
                _ => return Err(anyhow!("Expected array for context")),
            }
        }
        _ => return Err(anyhow!("Expected bytes for context")),
    };
    
    match variant_name {
        "EntityId" => {
            let entity_id = match &items[1] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid entity ID bytes"));
                    }
                    EntityId(u64::from_le_bytes(bytes.as_ref().try_into().unwrap()))
                }
                _ => return Err(anyhow!("Expected bytes for entity ID")),
            };
            let field_type = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid field type bytes"));
                    }
                    FieldType(u64::from_le_bytes(bytes.as_ref().try_into().unwrap()))
                }
                _ => return Err(anyhow!("Expected bytes for field type")),
            };
            Ok(NotifyConfig::EntityId { entity_id, field_type, trigger_on_change, context })
        }
        "EntityType" => {
            let entity_type = match &items[1] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 4 {
                        return Err(anyhow!("Invalid entity type bytes"));
                    }
                    EntityType(u32::from_le_bytes(bytes.as_ref().try_into().unwrap()))
                }
                _ => return Err(anyhow!("Expected bytes for entity type")),
            };
            let field_type = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid field type bytes"));
                    }
                    FieldType(u64::from_le_bytes(bytes.as_ref().try_into().unwrap()))
                }
                _ => return Err(anyhow!("Expected bytes for field type")),
            };
            Ok(NotifyConfig::EntityType { entity_type, field_type, trigger_on_change, context })
        }
        _ => Err(anyhow!("Unknown NotifyConfig variant: {}", variant_name)),
    }
}

// RESP encoding for EntitySchema<Single> - format: [entity_type_id, inherit_array, fields_map]
fn encode_entity_schema_single(schema: &EntitySchema<Single>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_array_header(&mut out, 3);
    
    // Encode entity type ID
    write_bulk(&mut out, &schema.entity_type.0.to_le_bytes());
    
    // Encode inherit array
    let mut inherit_data = Vec::new();
    write_array_header(&mut inherit_data, schema.inherit.len());
    for parent_type in &schema.inherit {
        write_bulk(&mut inherit_data, &parent_type.0.to_le_bytes());
    }
    write_bulk(&mut out, &inherit_data);
    
    // Encode fields map - as array of [field_type_id, field_schema] pairs
    let mut fields_data = Vec::new();
    write_array_header(&mut fields_data, schema.fields.len());
    for (field_type, field_schema) in &schema.fields {
        let mut field_pair = Vec::new();
        write_array_header(&mut field_pair, 2);
        write_bulk(&mut field_pair, &field_type.0.to_le_bytes());
        
        // Encode FieldSchema using our RESP encoding
        let field_schema_bytes = encode_field_schema(field_schema)?;
        write_bulk(&mut field_pair, &field_schema_bytes);
        
        fields_data.extend_from_slice(&field_pair);
    }
    write_bulk(&mut out, &fields_data);
    
    Ok(out)
}

fn decode_entity_schema_single(bytes: &Bytes) -> Result<EntitySchema<Single>> {
    let frame = parse_root_frame(bytes)?;
    let items = match frame {
        RespFrame::Array(items) => items,
        _ => return Err(anyhow!("Expected array for EntitySchema")),
    };
    
    if items.len() != 3 {
        return Err(anyhow!("EntitySchema array expects 3 elements"));
    }
    
    let entity_type = match &items[0] {
        RespFrame::Bulk(bytes) => {
            if bytes.len() != 4 {
                return Err(anyhow!("Invalid entity type bytes"));
            }
            EntityType(u32::from_le_bytes(bytes.as_ref().try_into().unwrap()))
        }
        _ => return Err(anyhow!("Expected bytes for entity type")),
    };
    
    let inherit = match &items[1] {
        RespFrame::Bulk(inherit_bytes) => {
            let inherit_frame = parse_root_frame(&Bytes::copy_from_slice(inherit_bytes))?;
            match inherit_frame {
                RespFrame::Array(inherit_items) => {
                    let mut inherit_vec = Vec::new();
                    for item in inherit_items {
                        match item {
                            RespFrame::Bulk(bytes) => {
                                if bytes.len() != 4 {
                                    return Err(anyhow!("Invalid entity type bytes"));
                                }
                                inherit_vec.push(EntityType(u32::from_le_bytes(bytes.as_ref().try_into().unwrap())));
                            }
                            _ => return Err(anyhow!("Expected bytes for inherit type")),
                        }
                    }
                    inherit_vec
                }
                _ => return Err(anyhow!("Expected array for inherit list")),
            }
        }
        _ => return Err(anyhow!("Expected bytes for inherit array")),
    };
    
    let fields = match &items[2] {
        RespFrame::Bulk(fields_bytes) => {
            let fields_frame = parse_root_frame(&Bytes::copy_from_slice(fields_bytes))?;
            match fields_frame {
                RespFrame::Array(field_items) => {
                    let mut fields_map = rustc_hash::FxHashMap::default();
                    for item in field_items {
                        match item {
                            RespFrame::Array(pair_items) => {
                                if pair_items.len() != 2 {
                                    return Err(anyhow!("Field pair expects 2 elements"));
                                }
                                let field_type = match &pair_items[0] {
                                    RespFrame::Bulk(bytes) => {
                                        if bytes.len() != 8 {
                                            return Err(anyhow!("Invalid field type bytes"));
                                        }
                                        FieldType(u64::from_le_bytes(bytes.as_ref().try_into().unwrap()))
                                    }
                                    _ => return Err(anyhow!("Expected bytes for field type")),
                                };
                                let field_schema = match &pair_items[1] {
                                    RespFrame::Bulk(schema_bytes) => {
                                        decode_field_schema(&Bytes::copy_from_slice(schema_bytes))?
                                    }
                                    _ => return Err(anyhow!("Expected bytes for field schema")),
                                };
                                fields_map.insert(field_type, field_schema);
                            }
                            _ => return Err(anyhow!("Expected array for field pair")),
                        }
                    }
                    fields_map
                }
                _ => return Err(anyhow!("Expected array for fields")),
            }
        }
        _ => return Err(anyhow!("Expected bytes for fields map")),
    };
    
    let mut schema: EntitySchema<Single> = EntitySchema::<Single>::new(entity_type, inherit);
    schema.fields = fields;
    Ok(schema)
}

// RESP encoding for EntitySchema<Complete> - format: [entity_type_id, inherit_array, fields_map]
fn encode_entity_schema_complete(schema: &EntitySchema<Complete>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_array_header(&mut out, 3);
    
    // Encode entity type ID
    write_bulk(&mut out, &schema.entity_type.0.to_le_bytes());
    
    // Encode inherit array
    let mut inherit_data = Vec::new();
    write_array_header(&mut inherit_data, schema.inherit.len());
    for parent_type in &schema.inherit {
        write_bulk(&mut inherit_data, &parent_type.0.to_le_bytes());
    }
    write_bulk(&mut out, &inherit_data);
    
    // Encode fields map - as array of [field_type_id, field_schema] pairs
    let mut fields_data = Vec::new();
    write_array_header(&mut fields_data, schema.fields.len());
    for (field_type, field_schema) in &schema.fields {
        let mut field_pair = Vec::new();
        write_array_header(&mut field_pair, 2);
        write_bulk(&mut field_pair, &field_type.0.to_le_bytes());
        
        // Encode FieldSchema using our RESP encoding
        let field_schema_bytes = encode_field_schema(field_schema)?;
        write_bulk(&mut field_pair, &field_schema_bytes);
        
        fields_data.extend_from_slice(&field_pair);
    }
    write_bulk(&mut out, &fields_data);
    
    Ok(out)
}

fn decode_entity_schema_complete(bytes: &Bytes) -> Result<EntitySchema<Complete>> {
    let frame = parse_root_frame(bytes)?;
    let items = match frame {
        RespFrame::Array(items) => items,
        _ => return Err(anyhow!("Expected array for EntitySchema")),
    };
    
    if items.len() != 3 {
        return Err(anyhow!("EntitySchema array expects 3 elements"));
    }
    
    let entity_type = match &items[0] {
        RespFrame::Bulk(bytes) => {
            if bytes.len() != 4 {
                return Err(anyhow!("Invalid entity type bytes"));
            }
            EntityType(u32::from_le_bytes(bytes.as_ref().try_into().unwrap()))
        }
        _ => return Err(anyhow!("Expected bytes for entity type")),
    };
    
    let inherit = match &items[1] {
        RespFrame::Bulk(inherit_bytes) => {
            let inherit_frame = parse_root_frame(&Bytes::copy_from_slice(inherit_bytes))?;
            match inherit_frame {
                RespFrame::Array(inherit_items) => {
                    let mut inherit_vec = Vec::new();
                    for item in inherit_items {
                        match item {
                            RespFrame::Bulk(bytes) => {
                                if bytes.len() != 4 {
                                    return Err(anyhow!("Invalid entity type bytes"));
                                }
                                inherit_vec.push(EntityType(u32::from_le_bytes(bytes.as_ref().try_into().unwrap())));
                            }
                            _ => return Err(anyhow!("Expected bytes for inherit type")),
                        }
                    }
                    inherit_vec
                }
                _ => return Err(anyhow!("Expected array for inherit list")),
            }
        }
        _ => return Err(anyhow!("Expected bytes for inherit array")),
    };
    
    let fields = match &items[2] {
        RespFrame::Bulk(fields_bytes) => {
            let fields_frame = parse_root_frame(&Bytes::copy_from_slice(fields_bytes))?;
            match fields_frame {
                RespFrame::Array(field_items) => {
                    let mut fields_map = rustc_hash::FxHashMap::default();
                    for item in field_items {
                        match item {
                            RespFrame::Array(pair_items) => {
                                if pair_items.len() != 2 {
                                    return Err(anyhow!("Field pair expects 2 elements"));
                                }
                                let field_type = match &pair_items[0] {
                                    RespFrame::Bulk(bytes) => {
                                        if bytes.len() != 8 {
                                            return Err(anyhow!("Invalid field type bytes"));
                                        }
                                        FieldType(u64::from_le_bytes(bytes.as_ref().try_into().unwrap()))
                                    }
                                    _ => return Err(anyhow!("Expected bytes for field type")),
                                };
                                let field_schema = match &pair_items[1] {
                                    RespFrame::Bulk(schema_bytes) => {
                                        decode_field_schema(&Bytes::copy_from_slice(schema_bytes))?
                                    }
                                    _ => return Err(anyhow!("Expected bytes for field schema")),
                                };
                                fields_map.insert(field_type, field_schema);
                            }
                            _ => return Err(anyhow!("Expected array for field pair")),
                        }
                    }
                    fields_map
                }
                _ => return Err(anyhow!("Expected array for fields")),
            }
        }
        _ => return Err(anyhow!("Expected bytes for fields map")),
    };
    
    let schema: EntitySchema<Complete> = EntitySchema::<Single>::new(entity_type, inherit).into();
    let mut complete_schema = schema;
    complete_schema.fields = fields;
    Ok(complete_schema)
}
pub fn encode_entity_schema_string(schema: &EntitySchema<Single, String, String>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    write_array_header(&mut out, 3);
    
    // Encode entity type name
    write_bulk(&mut out, schema.entity_type.as_bytes());
    
    // Encode inherit array
    let mut inherit_data = Vec::new();
    write_array_header(&mut inherit_data, schema.inherit.len());
    for parent_type in &schema.inherit {
        write_bulk(&mut inherit_data, parent_type.as_bytes());
    }
    write_bulk(&mut out, &inherit_data);
    
    // Encode fields map - as array of [field_name, field_schema] pairs
    let mut fields_data = Vec::new();
    write_array_header(&mut fields_data, schema.fields.len());
    for (field_name, field_schema) in &schema.fields {
        let mut field_pair = Vec::new();
        write_array_header(&mut field_pair, 2);
        write_bulk(&mut field_pair, field_name.as_bytes());
        
        // Convert FieldSchema<String> to bytes by encoding it
        let field_schema_bytes = encode_field_schema_string(field_schema)?;
        write_bulk(&mut field_pair, &field_schema_bytes);
        
        fields_data.extend_from_slice(&field_pair);
    }
    write_bulk(&mut out, &fields_data);
    
    Ok(out)
}

fn decode_entity_schema_string(bytes: &Bytes) -> Result<EntitySchema<Single, String, String>> {
    let frame = parse_root_frame(bytes)?;
    let items = match frame {
        RespFrame::Array(items) => items,
        _ => return Err(anyhow!("Expected array for EntitySchema")),
    };
    
    if items.len() != 3 {
        return Err(anyhow!("EntitySchema array expects 3 elements"));
    }
    
    let entity_type = match &items[0] {
        RespFrame::Bulk(bytes) => String::from_utf8(bytes.to_vec())?,
        _ => return Err(anyhow!("Expected string for entity type")),
    };
    
    let inherit = match &items[1] {
        RespFrame::Bulk(inherit_bytes) => {
            let inherit_frame = parse_root_frame(&Bytes::copy_from_slice(inherit_bytes))?;
            match inherit_frame {
                RespFrame::Array(inherit_items) => {
                    let mut inherit_vec = Vec::new();
                    for item in inherit_items {
                        match item {
                            RespFrame::Bulk(bytes) => {
                                inherit_vec.push(String::from_utf8(bytes.to_vec())?);
                            }
                            _ => return Err(anyhow!("Expected string for inherit type")),
                        }
                    }
                    inherit_vec
                }
                _ => return Err(anyhow!("Expected array for inherit list")),
            }
        }
        _ => return Err(anyhow!("Expected bytes for inherit array")),
    };
    
    let fields = match &items[2] {
        RespFrame::Bulk(fields_bytes) => {
            let fields_frame = parse_root_frame(&Bytes::copy_from_slice(fields_bytes))?;
            match fields_frame {
                RespFrame::Array(field_items) => {
                    let mut fields_map = rustc_hash::FxHashMap::default();
                    for item in field_items {
                        match item {
                            RespFrame::Array(pair_items) => {
                                if pair_items.len() != 2 {
                                    return Err(anyhow!("Field pair expects 2 elements"));
                                }
                                let field_name = match &pair_items[0] {
                                    RespFrame::Bulk(bytes) => String::from_utf8(bytes.to_vec())?,
                                    _ => return Err(anyhow!("Expected string for field name")),
                                };
                                let field_schema = match &pair_items[1] {
                                    RespFrame::Bulk(schema_bytes) => {
                                        decode_field_schema_string(&Bytes::copy_from_slice(schema_bytes))?
                                    }
                                    _ => return Err(anyhow!("Expected bytes for field schema")),
                                };
                                fields_map.insert(field_name, field_schema);
                            }
                            _ => return Err(anyhow!("Expected array for field pair")),
                        }
                    }
                    fields_map
                }
                _ => return Err(anyhow!("Expected array for fields")),
            }
        }
        _ => return Err(anyhow!("Expected bytes for fields map")),
    };

    let mut schema: EntitySchema<Single, String, String> = EntitySchema::<Single, String, String>::new(entity_type, inherit);
    schema.fields = fields;
    Ok(schema)
}

// Helper function to encode FieldSchema<String> (string-based field schema)
fn encode_field_schema_string(schema: &crate::FieldSchema<String>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    
    match schema {
        crate::FieldSchema::String { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"String");
            write_bulk(&mut out, field_type.as_bytes());
            write_bulk(&mut out, default_value.as_bytes());
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        crate::FieldSchema::Int { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Int");
            write_bulk(&mut out, field_type.as_bytes());
            write_bulk(&mut out, &default_value.to_le_bytes());
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        crate::FieldSchema::Float { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Float");
            write_bulk(&mut out, field_type.as_bytes());
            write_bulk(&mut out, &default_value.to_le_bytes());
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        crate::FieldSchema::Bool { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Bool");
            write_bulk(&mut out, field_type.as_bytes());
            write_bulk(&mut out, &[if *default_value { 1 } else { 0 }]);
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        crate::FieldSchema::Blob { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Blob");
            write_bulk(&mut out, field_type.as_bytes());
            write_bulk(&mut out, default_value);
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        crate::FieldSchema::EntityReference { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"EntityReference");
            write_bulk(&mut out, field_type.as_bytes());
            if let Some(entity_id) = default_value {
                write_bulk(&mut out, &entity_id.0.to_le_bytes());
            } else {
                write_null(&mut out);
            }
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        crate::FieldSchema::EntityList { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"EntityList");
            write_bulk(&mut out, field_type.as_bytes());
            let mut list_data = Vec::new();
            write_array_header(&mut list_data, default_value.len());
            for entity_id in default_value {
                write_bulk(&mut list_data, &entity_id.0.to_le_bytes());
            }
            write_bulk(&mut out, &list_data);
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        crate::FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope } => {
            write_array_header(&mut out, 6);
            write_bulk(&mut out, b"Choice");
            write_bulk(&mut out, field_type.as_bytes());
            write_bulk(&mut out, &default_value.to_le_bytes());
            write_integer(&mut out, *rank);
            let mut choices_data = Vec::new();
            write_array_header(&mut choices_data, choices.len());
            for choice in choices {
                write_bulk(&mut choices_data, choice.as_bytes());
            }
            write_bulk(&mut out, &choices_data);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
        crate::FieldSchema::Timestamp { field_type, default_value, rank, storage_scope } => {
            write_array_header(&mut out, 5);
            write_bulk(&mut out, b"Timestamp");
            write_bulk(&mut out, field_type.as_bytes());
            let unix_nanos = default_value.unix_timestamp_nanos() as u128;
            write_bulk(&mut out, &unix_nanos.to_le_bytes());
            write_integer(&mut out, *rank);
            write_bulk(&mut out, match storage_scope {
                StorageScope::Runtime => b"Runtime",
                StorageScope::Configuration => b"Configuration",
            });
        }
    }
    
    Ok(out)
}

fn decode_field_schema_string(bytes: &Bytes) -> Result<crate::FieldSchema<String>> {
    let frame = parse_root_frame(bytes)?;
    let items = match frame {
        RespFrame::Array(items) => items,
        _ => return Err(anyhow!("Expected array for FieldSchema")),
    };
    
    if items.len() < 5 {
        return Err(anyhow!("FieldSchema array too short"));
    }
    
    let type_name = match &items[0] {
        RespFrame::Bulk(bytes) => std::str::from_utf8(bytes)?,
        _ => return Err(anyhow!("Expected string for FieldSchema type name")),
    };
    
    let field_type = match &items[1] {
        RespFrame::Bulk(bytes) => String::from_utf8(bytes.to_vec())?,
        _ => return Err(anyhow!("Expected string for field type")),
    };
    
    let rank = match &items[3] {
        RespFrame::Integer(i) => *i,
        _ => return Err(anyhow!("Expected integer for rank")),
    };
    
    let storage_scope = match &items[4] {
        RespFrame::Bulk(bytes) => {
            match std::str::from_utf8(bytes)? {
                "Runtime" => StorageScope::Runtime,
                "Configuration" => StorageScope::Configuration,
                s => return Err(anyhow!("Unknown storage scope: {}", s)),
            }
        }
        _ => return Err(anyhow!("Expected string for storage scope")),
    };
    
    match type_name {
        "String" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => String::from_utf8(bytes.to_vec())?,
                _ => return Err(anyhow!("Expected string for default value")),
            };
            Ok(crate::FieldSchema::String { field_type, default_value, rank, storage_scope })
        }
        "Int" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid int bytes"));
                    }
                    i64::from_le_bytes(bytes.as_ref().try_into().unwrap())
                }
                _ => return Err(anyhow!("Expected bytes for int default value")),
            };
            Ok(crate::FieldSchema::Int { field_type, default_value, rank, storage_scope })
        }
        "Float" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid float bytes"));
                    }
                    f64::from_le_bytes(bytes.as_ref().try_into().unwrap())
                }
                _ => return Err(anyhow!("Expected bytes for float default value")),
            };
            Ok(crate::FieldSchema::Float { field_type, default_value, rank, storage_scope })
        }
        "Bool" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 1 {
                        return Err(anyhow!("Invalid bool bytes"));
                    }
                    bytes[0] != 0
                }
                _ => return Err(anyhow!("Expected bytes for bool default value")),
            };
            Ok(crate::FieldSchema::Bool { field_type, default_value, rank, storage_scope })
        }
        "Blob" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => bytes.to_vec(),
                _ => return Err(anyhow!("Expected bytes for blob default value")),
            };
            Ok(crate::FieldSchema::Blob { field_type, default_value, rank, storage_scope })
        }
        "EntityReference" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid entity reference bytes"));
                    }
                    Some(EntityId(u64::from_le_bytes(bytes.as_ref().try_into().unwrap())))
                }
                RespFrame::Null => None,
                _ => return Err(anyhow!("Expected bytes or null for entity reference default value")),
            };
            Ok(crate::FieldSchema::EntityReference { field_type, default_value, rank, storage_scope })
        }
        "EntityList" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(list_bytes) => {
                    let list_frame = parse_root_frame(&Bytes::copy_from_slice(list_bytes))?;
                    match list_frame {
                        RespFrame::Array(list_items) => {
                            let mut entity_ids = Vec::new();
                            for item in list_items {
                                match item {
                                    RespFrame::Bulk(bytes) => {
                                        if bytes.len() != 8 {
                                            return Err(anyhow!("Invalid entity ID bytes"));
                                        }
                                        entity_ids.push(EntityId(u64::from_le_bytes(bytes.as_ref().try_into().unwrap())));
                                    }
                                    _ => return Err(anyhow!("Expected bytes for entity ID")),
                                }
                            }
                            entity_ids
                        }
                        _ => return Err(anyhow!("Expected array for entity list")),
                    }
                }
                _ => return Err(anyhow!("Expected bytes for entity list default value")),
            };
            Ok(crate::FieldSchema::EntityList { field_type, default_value, rank, storage_scope })
        }
        "Choice" => {
            if items.len() != 6 {
                return Err(anyhow!("Choice FieldSchema expects 6 elements"));
            }
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 8 {
                        return Err(anyhow!("Invalid choice bytes"));
                    }
                    i64::from_le_bytes(bytes.as_ref().try_into().unwrap())
                }
                _ => return Err(anyhow!("Expected bytes for choice default value")),
            };
            let choices = match &items[4] {
                RespFrame::Bulk(choices_bytes) => {
                    let choices_frame = parse_root_frame(&Bytes::copy_from_slice(choices_bytes))?;
                    match choices_frame {
                        RespFrame::Array(choice_items) => {
                            let mut choice_strings = Vec::new();
                            for item in choice_items {
                                match item {
                                    RespFrame::Bulk(bytes) => {
                                        choice_strings.push(String::from_utf8(bytes.to_vec())?);
                                    }
                                    _ => return Err(anyhow!("Expected bytes for choice string")),
                                }
                            }
                            choice_strings
                        }
                        _ => return Err(anyhow!("Expected array for choices")),
                    }
                }
                _ => return Err(anyhow!("Expected bytes for choices")),
            };
            Ok(crate::FieldSchema::Choice { field_type, default_value, rank, choices, storage_scope })
        }
        "Timestamp" => {
            let default_value = match &items[2] {
                RespFrame::Bulk(bytes) => {
                    if bytes.len() != 16 {
                        return Err(anyhow!("Invalid timestamp bytes"));
                    }
                    let unix_nanos = u128::from_le_bytes(bytes.as_ref().try_into().unwrap());
                    crate::nanos_to_timestamp(unix_nanos as u64)
                }
                _ => return Err(anyhow!("Expected bytes for timestamp default value")),
            };
            Ok(crate::FieldSchema::Timestamp { field_type, default_value, rank, storage_scope })
        }
        _ => Err(anyhow!("Unknown FieldSchema type: {}", type_name)),
    }
}

fn parse_adjust_behavior(bytes: &Bytes) -> Result<AdjustBehavior> {
    let s = parse_str(bytes)?;
    match s {
        "Set" => Ok(AdjustBehavior::Set),
        "Add" => Ok(AdjustBehavior::Add),
        "Subtract" => Ok(AdjustBehavior::Subtract),
        _ => Err(anyhow!("invalid AdjustBehavior: {}", s)),
    }
}

#[derive(Debug)]
pub enum StoreCommand<'a> {
    GetEntityType { name: &'a str },
    ResolveEntityType { entity_type: EntityType },
    GetFieldType { name: &'a str },
    ResolveFieldType { field_type: FieldType },
    GetEntitySchema { entity_type: EntityType },
    GetCompleteEntitySchema { entity_type: EntityType },
    GetFieldSchema { entity_type: EntityType, field_type: FieldType },
    SetFieldSchema { entity_type: EntityType, field_type: FieldType, schema: FieldSchema },
    EntityExists { entity_id: EntityId },
    FieldExists { entity_type: EntityType, field_type: FieldType },
    ResolveIndirection { entity_id: EntityId, field_path: Vec<FieldType> },
    Read { entity_id: EntityId, field_path: Vec<FieldType> },
    Write { entity_id: EntityId, field_path: Vec<FieldType>, value: Value, writer_id: Option<EntityId>, write_time: Option<Timestamp>, push_condition: Option<PushCondition>, adjust_behavior: Option<AdjustBehavior> },
    CreateEntity { entity_type: EntityType, parent_id: Option<EntityId>, name: String },
    DeleteEntity { entity_id: EntityId },
    UpdateSchema { schema: EntitySchema<Single, String, String> },
    TakeSnapshot,
    FindEntitiesPaginated { entity_type: EntityType, page_opts: Option<PageOpts>, filter: Option<String> },
    FindEntitiesExact { entity_type: EntityType, page_opts: Option<PageOpts>, filter: Option<String> },
    FindEntities { entity_type: EntityType, filter: Option<String> },
    GetEntityTypes,
    GetEntityTypesPaginated { page_opts: Option<PageOpts> },
    RegisterNotification { config: NotifyConfig },
    UnregisterNotification { config: NotifyConfig },
}

/// Helper trait for command argument parsing and validation
pub trait CommandArguments {
    fn expect_args(&self, count: usize, command_name: &str) -> Result<()>;
    fn expect_args_range(&self, min: usize, max: usize, command_name: &str) -> Result<()>;
}

impl CommandArguments for QuspCommand {
    fn expect_args(&self, count: usize, command_name: &str) -> Result<()> {
        if self.args.len() != count {
            return Err(anyhow!("{} expects {} argument{}", 
                command_name, count, if count == 1 { "" } else { "s" }));
        }
        Ok(())
    }
    
    fn expect_args_range(&self, min: usize, max: usize, command_name: &str) -> Result<()> {
        if self.args.len() < min || self.args.len() > max {
            return Err(anyhow!("{} expects {}-{} arguments", command_name, min, max));
        }
        Ok(())
    }
}

/// Command-specific parsers
pub mod command_parsers {
    use super::*;
    
    pub fn parse_get_entity_type(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "GET_ENTITY_TYPE")?;
        let name = parse_str(&cmd.args[0])?;
        Ok(StoreCommand::GetEntityType { name })
    }
    
    pub fn parse_resolve_entity_type(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "RESOLVE_ENTITY_TYPE")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        Ok(StoreCommand::ResolveEntityType { entity_type })
    }
    
    pub fn parse_get_field_type(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "GET_FIELD_TYPE")?;
        let name = parse_str(&cmd.args[0])?;
        Ok(StoreCommand::GetFieldType { name })
    }
    
    pub fn parse_resolve_field_type(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "RESOLVE_FIELD_TYPE")?;
        let field_type = parse_field_type(&cmd.args[0])?;
        Ok(StoreCommand::ResolveFieldType { field_type })
    }
    
    pub fn parse_get_entity_schema(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "GET_ENTITY_SCHEMA")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        Ok(StoreCommand::GetEntitySchema { entity_type })
    }
    
    pub fn parse_get_complete_entity_schema(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "GET_COMPLETE_ENTITY_SCHEMA")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        Ok(StoreCommand::GetCompleteEntitySchema { entity_type })
    }
    
    pub fn parse_get_field_schema(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(2, "GET_FIELD_SCHEMA")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        let field_type = parse_field_type(&cmd.args[1])?;
        Ok(StoreCommand::GetFieldSchema { entity_type, field_type })
    }
    
    pub fn parse_entity_exists(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "ENTITY_EXISTS")?;
        let entity_id = parse_entity_id(&cmd.args[0])?;
        Ok(StoreCommand::EntityExists { entity_id })
    }
    
    pub fn parse_field_exists(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(2, "FIELD_EXISTS")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        let field_type = parse_field_type(&cmd.args[1])?;
        Ok(StoreCommand::FieldExists { entity_type, field_type })
    }
    
    pub fn parse_read(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(2, "READ")?;
        let entity_id = parse_entity_id(&cmd.args[0])?;
        let field_path = parse_field_path(&cmd.args[1])?;
        Ok(StoreCommand::Read { entity_id, field_path })
    }
    
    pub fn parse_get_entity_types(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(0, "GET_ENTITY_TYPES")?;
        Ok(StoreCommand::GetEntityTypes)
    }
    
    pub fn parse_take_snapshot(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(0, "TAKE_SNAPSHOT")?;
        Ok(StoreCommand::TakeSnapshot)
    }
    
    pub fn parse_set_field_schema(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(3, "SET_FIELD_SCHEMA")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        let field_type = parse_field_type(&cmd.args[1])?;
        let schema = decode_field_schema(&cmd.args[2])?;
        Ok(StoreCommand::SetFieldSchema { entity_type, field_type, schema })
    }
    
    pub fn parse_resolve_indirection(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(2, "RESOLVE_INDIRECTION")?;
        let entity_id = parse_entity_id(&cmd.args[0])?;
        let field_path = parse_field_path(&cmd.args[1])?;
        Ok(StoreCommand::ResolveIndirection { entity_id, field_path })
    }
    
    pub fn parse_write(cmd: &QuspCommand) -> Result<StoreCommand> {
        if cmd.args.len() < 3 {
            return Err(anyhow!("WRITE expects at least 3 arguments"));
        }
        let entity_id = parse_entity_id(&cmd.args[0])?;
        let field_path = parse_field_path(&cmd.args[1])?;
        let value = decode_value(&cmd.args[2])?;
        
        let mut writer_id = None;
        let mut write_time = None;
        let mut push_condition = None;
        let mut adjust_behavior = None;
        
        // Parse optional arguments based on their position
        let mut idx = 3;
        if idx < cmd.args.len() {
            // Writer ID (optional)
            let writer_str = parse_str(&cmd.args[idx])?;
            if writer_str != "null" {
                writer_id = Some(parse_entity_id(&cmd.args[idx])?);
            }
            idx += 1;
        }
        if idx < cmd.args.len() {
            // Write time (optional)
            let time_str = parse_str(&cmd.args[idx])?;
            if time_str != "null" {
                write_time = Some(parse_timestamp(time_str)?);
            }
            idx += 1;
        }
        if idx < cmd.args.len() {
            // Push condition (optional)
            let cond_str = parse_str(&cmd.args[idx])?;
            if cond_str != "null" {
                push_condition = Some(match cond_str {
                    "Always" => PushCondition::Always,
                    "Changes" => PushCondition::Changes,
                    _ => return Err(anyhow!("invalid PushCondition: {}", cond_str)),
                });
            }
            idx += 1;
        }
        if idx < cmd.args.len() {
            // Adjust behavior (optional)
            let adjust_str = parse_str(&cmd.args[idx])?;
            if adjust_str != "null" {
                adjust_behavior = Some(parse_adjust_behavior(&cmd.args[idx])?);
            }
        }
        
        Ok(StoreCommand::Write { entity_id, field_path, value, writer_id, write_time, push_condition, adjust_behavior })
    }
    
    pub fn parse_create_entity(cmd: &QuspCommand) -> Result<StoreCommand> {
        if cmd.args.len() < 3 {
            return Err(anyhow!("CREATE_ENTITY expects at least 3 arguments"));
        }
        let entity_type = parse_entity_type(&cmd.args[0])?;
        let parent_id_str = parse_str(&cmd.args[1])?;
        let parent_id = if parent_id_str == "null" {
            None
        } else {
            Some(parse_entity_id(&cmd.args[1])?)
        };
        let name = parse_str(&cmd.args[2])?.to_string();
        Ok(StoreCommand::CreateEntity { entity_type, parent_id, name })
    }
    
    pub fn parse_delete_entity(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "DELETE_ENTITY")?;
        let entity_id = parse_entity_id(&cmd.args[0])?;
        Ok(StoreCommand::DeleteEntity { entity_id })
    }
    
    pub fn parse_update_schema(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "UPDATE_SCHEMA")?;
        let schema = decode_entity_schema_string(&cmd.args[0])?;
        Ok(StoreCommand::UpdateSchema { schema })
    }
    
    // Helper function for parsing optional arguments with null handling
    fn parse_optional_page_opts(cmd: &QuspCommand, index: usize) -> Result<Option<PageOpts>> {
        if index >= cmd.args.len() {
            return Ok(None);
        }
        let opts_str = parse_str(&cmd.args[index])?;
        if opts_str == "null" {
            Ok(None)
        } else {
            Ok(Some(decode_page_opts(&cmd.args[index])?))
        }
    }
    
    fn parse_optional_filter(cmd: &QuspCommand, index: usize) -> Result<Option<String>> {
        if index >= cmd.args.len() {
            return Ok(None);
        }
        let filter_str = parse_str(&cmd.args[index])?;
        if filter_str == "null" {
            Ok(None)
        } else {
            Ok(Some(filter_str.to_string()))
        }
    }
    
    pub fn parse_find_entities_paginated(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args_range(1, 3, "FIND_ENTITIES_PAGINATED")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        let page_opts = parse_optional_page_opts(cmd, 1)?;
        let filter = parse_optional_filter(cmd, 2)?;
        Ok(StoreCommand::FindEntitiesPaginated { entity_type, page_opts, filter })
    }
    
    pub fn parse_find_entities_exact(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args_range(1, 3, "FIND_ENTITIES_EXACT")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        let page_opts = parse_optional_page_opts(cmd, 1)?;
        let filter = parse_optional_filter(cmd, 2)?;
        Ok(StoreCommand::FindEntitiesExact { entity_type, page_opts, filter })
    }
    
    pub fn parse_find_entities(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args_range(1, 2, "FIND_ENTITIES")?;
        let entity_type = parse_entity_type(&cmd.args[0])?;
        let filter = parse_optional_filter(cmd, 1)?;
        Ok(StoreCommand::FindEntities { entity_type, filter })
    }
    
    pub fn parse_get_entity_types_paginated(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args_range(0, 1, "GET_ENTITY_TYPES_PAGINATED")?;
        let page_opts = parse_optional_page_opts(cmd, 0)?;
        Ok(StoreCommand::GetEntityTypesPaginated { page_opts })
    }
    
    pub fn parse_register_notification(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "REGISTER_NOTIFICATION")?;
        let config = decode_notify_config(&cmd.args[0])?;
        Ok(StoreCommand::RegisterNotification { config })
    }
    
    pub fn parse_unregister_notification(cmd: &QuspCommand) -> Result<StoreCommand> {
        cmd.expect_args(1, "UNREGISTER_NOTIFICATION")?;
        let config = decode_notify_config(&cmd.args[0])?;
        Ok(StoreCommand::UnregisterNotification { config })
    }
}

pub fn parse_store_command(cmd: &QuspCommand) -> Result<StoreCommand> {
    use command_parsers::*;
    
    let name = cmd.uppercase_name()?;
    match name.as_str() {
        "GET_ENTITY_TYPE" => parse_get_entity_type(cmd),
        "RESOLVE_ENTITY_TYPE" => parse_resolve_entity_type(cmd),
        "GET_FIELD_TYPE" => parse_get_field_type(cmd),
        "RESOLVE_FIELD_TYPE" => parse_resolve_field_type(cmd),
        "GET_ENTITY_SCHEMA" => parse_get_entity_schema(cmd),
        "GET_COMPLETE_ENTITY_SCHEMA" => parse_get_complete_entity_schema(cmd),
        "GET_FIELD_SCHEMA" => parse_get_field_schema(cmd),
        "SET_FIELD_SCHEMA" => parse_set_field_schema(cmd),
        "ENTITY_EXISTS" => parse_entity_exists(cmd),
        "FIELD_EXISTS" => parse_field_exists(cmd),
        "RESOLVE_INDIRECTION" => parse_resolve_indirection(cmd),
        "READ" => parse_read(cmd),
        "WRITE" => parse_write(cmd),
        "CREATE_ENTITY" => parse_create_entity(cmd),
        "DELETE_ENTITY" => parse_delete_entity(cmd),
        "UPDATE_SCHEMA" => parse_update_schema(cmd),
        "TAKE_SNAPSHOT" => parse_take_snapshot(cmd),
        "FIND_ENTITIES_PAGINATED" => parse_find_entities_paginated(cmd),
        "FIND_ENTITIES_EXACT" => parse_find_entities_exact(cmd),
        "FIND_ENTITIES" => parse_find_entities(cmd),
        "GET_ENTITY_TYPES" => parse_get_entity_types(cmd),
        "GET_ENTITY_TYPES_PAGINATED" => parse_get_entity_types_paginated(cmd),
        "REGISTER_NOTIFICATION" => parse_register_notification(cmd),
        "UNREGISTER_NOTIFICATION" => parse_unregister_notification(cmd),
        _ => Err(anyhow!("unknown command: {}", name)),
    }
}

pub fn encode_response(response: &QuspResponse) -> Vec<u8> {
    let mut out = Vec::new();
    write_response(&mut out, response);
    out
}

fn parse_field_path(bytes: &Bytes) -> Result<Vec<FieldType>> {
    let s = parse_str(bytes)?;
    if s.is_empty() {
        return Ok(vec![]);
    }
    s.split(',').map(|part| {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("empty field type in path"));
        }
        parse_field_type_str(trimmed)
    }).collect::<Result<Vec<_>>>()
}

fn parse_field_type_str(s: &str) -> Result<FieldType> {
    s.trim().parse().map_err(|e| anyhow!("invalid field type: {}", e)).map(FieldType)
}

// Response encoding functions
pub fn encode_entity_type_response(entity_type: EntityType) -> QuspResponse {
    QuspResponse::Integer(entity_type.0 as i64)
}

pub fn encode_entity_type_name_response(name: &str) -> QuspResponse {
    QuspResponse::Bulk(Bytes::copy_from_slice(name.as_bytes()))
}

pub fn encode_field_type_response(field_type: FieldType) -> QuspResponse {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&field_type.0.to_le_bytes());
    QuspResponse::Bulk(Bytes::copy_from_slice(&buf))
}

pub fn encode_field_type_name_response(name: &str) -> QuspResponse {
    QuspResponse::Bulk(Bytes::copy_from_slice(name.as_bytes()))
}

pub fn encode_entity_schema_response(schema: &EntitySchema<Single>) -> Result<QuspResponse> {
    let encoded = encode_entity_schema_single(schema)?;
    Ok(QuspResponse::Bulk(Bytes::copy_from_slice(&encoded)))
}

pub fn encode_complete_entity_schema_response(schema: &EntitySchema<Complete>) -> Result<QuspResponse> {
    let encoded = encode_entity_schema_complete(schema)?;
    Ok(QuspResponse::Bulk(Bytes::copy_from_slice(&encoded)))
}

pub fn encode_field_schema_response(schema: &FieldSchema) -> Result<QuspResponse> {
    let encoded = encode_field_schema(schema)?;
    Ok(QuspResponse::Bulk(Bytes::copy_from_slice(&encoded)))
}

pub fn encode_bool_response(value: bool) -> QuspResponse {
    QuspResponse::Integer(if value { 1 } else { 0 })
}

pub fn encode_indirection_response(entity_id: EntityId, field_type: FieldType) -> QuspResponse {
    let mut response = Vec::new();
    response.push(QuspResponse::Integer(entity_id.0 as i64));
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&field_type.0.to_le_bytes());
    response.push(QuspResponse::Bulk(Bytes::copy_from_slice(&buf)));
    QuspResponse::Array(response)
}

pub fn encode_read_response(value: &Value, timestamp: Timestamp, writer_id: Option<EntityId>) -> QuspResponse {
    let mut response = Vec::new();
    let encoded_value = encode_value(value);
    response.push(QuspResponse::Bulk(Bytes::copy_from_slice(&encoded_value)));
    response.push(QuspResponse::Integer(timestamp.unix_timestamp_nanos() as i64));
    if let Some(writer) = writer_id {
        response.push(QuspResponse::Integer(writer.0 as i64));
    } else {
        response.push(QuspResponse::Null);
    }
    QuspResponse::Array(response)
}

pub fn encode_entity_id_response(entity_id: EntityId) -> QuspResponse {
    QuspResponse::Integer(entity_id.0 as i64)
}

pub fn encode_snapshot_response(snapshot: &Snapshot) -> Result<QuspResponse> {
    let encoded = bincode::serialize(snapshot).map_err(|e| anyhow!("failed to encode Snapshot: {}", e))?;
    Ok(QuspResponse::Bulk(Bytes::copy_from_slice(&encoded)))
}

pub fn encode_page_result_entity_ids(result: &PageResult<EntityId>) -> QuspResponse {
    let mut response = Vec::new();
    let mut items = Vec::new();
    for entity_id in &result.items {
        items.push(QuspResponse::Integer(entity_id.0 as i64));
    }
    response.push(QuspResponse::Array(items));
    response.push(QuspResponse::Integer(result.total as i64));
    if let Some(cursor) = result.next_cursor {
        response.push(QuspResponse::Integer(cursor as i64));
    } else {
        response.push(QuspResponse::Null);
    }
    QuspResponse::Array(response)
}

pub fn encode_page_result_entity_types(result: &PageResult<EntityType>) -> QuspResponse {
    let mut response = Vec::new();
    let mut items = Vec::new();
    for entity_type in &result.items {
        items.push(QuspResponse::Integer(entity_type.0 as i64));
    }
    response.push(QuspResponse::Array(items));
    response.push(QuspResponse::Integer(result.total as i64));
    if let Some(cursor) = result.next_cursor {
        response.push(QuspResponse::Integer(cursor as i64));
    } else {
        response.push(QuspResponse::Null);
    }
    QuspResponse::Array(response)
}

pub fn encode_entity_ids_response(entity_ids: &[EntityId]) -> QuspResponse {
    let mut items = Vec::new();
    for entity_id in entity_ids {
        items.push(QuspResponse::Integer(entity_id.0 as i64));
    }
    QuspResponse::Array(items)
}

pub fn encode_entity_types_response(entity_types: &[EntityType]) -> QuspResponse {
    let mut items = Vec::new();
    for entity_type in entity_types {
        items.push(QuspResponse::Integer(entity_type.0 as i64));
    }
    QuspResponse::Array(items)
}

// Response parsing functions for zero-copy deserialization
pub fn parse_entity_type_response(response: QuspResponse) -> Result<EntityType> {
    match response {
        QuspResponse::Integer(i) => Ok(EntityType(i as u32)),
        _ => Err(anyhow!("Expected integer for EntityType")),
    }
}

pub fn parse_field_type_response(response: QuspResponse) -> Result<FieldType> {
    match response {
        QuspResponse::Bulk(bytes) => {
            if bytes.len() != 8 {
                return Err(anyhow!("Invalid FieldType bytes"));
            }
            Ok(FieldType(u64::from_le_bytes(bytes.as_ref().try_into().unwrap())))
        }
        _ => Err(anyhow!("Expected bulk bytes for FieldType")),
    }
}

pub fn parse_string_response(response: QuspResponse) -> Result<String> {
    match response {
        QuspResponse::Bulk(bytes) => {
            String::from_utf8(bytes.to_vec()).map_err(|e| anyhow!("Invalid UTF-8: {}", e))
        }
        QuspResponse::Simple(bytes) => {
            String::from_utf8(bytes.to_vec()).map_err(|e| anyhow!("Invalid UTF-8: {}", e))
        }
        _ => Err(anyhow!("Expected bulk or simple string")),
    }
}

pub fn parse_bool_response(response: QuspResponse) -> Result<bool> {
    match response {
        QuspResponse::Integer(i) => Ok(i != 0),
        _ => Err(anyhow!("Expected integer for boolean")),
    }
}

pub fn parse_entity_id_response(response: QuspResponse) -> Result<EntityId> {
    match response {
        QuspResponse::Integer(i) => Ok(EntityId(i as u64)),
        _ => Err(anyhow!("Expected integer for EntityId")),
    }
}

pub fn parse_indirection_response(response: QuspResponse) -> Result<(EntityId, FieldType)> {
    match response {
        QuspResponse::Array(items) => {
            if items.len() != 2 {
                return Err(anyhow!("Indirection response expects 2 elements"));
            }
            let entity_id = parse_entity_id_response(items[0].clone())?;
            let field_type = parse_field_type_response(items[1].clone())?;
            Ok((entity_id, field_type))
        }
        _ => Err(anyhow!("Expected array for indirection response")),
    }
}

pub fn parse_read_response(response: QuspResponse) -> Result<(Value, Timestamp, Option<EntityId>)> {
    match response {
        QuspResponse::Array(items) => {
            if items.len() != 3 {
                return Err(anyhow!("Read response expects 3 elements"));
            }
            let value = match &items[0] {
                QuspResponse::Bulk(bytes) => decode_value(bytes)?,
                _ => return Err(anyhow!("Expected bulk bytes for value")),
            };
            let timestamp = match &items[1] {
                QuspResponse::Integer(nanos) => crate::nanos_to_timestamp(*nanos as u64),
                _ => return Err(anyhow!("Expected integer for timestamp")),
            };
            let writer_id = match &items[2] {
                QuspResponse::Integer(id) => Some(EntityId(*id as u64)),
                QuspResponse::Null => None,
                _ => return Err(anyhow!("Expected integer or null for writer_id")),
            };
            Ok((value, timestamp, writer_id))
        }
        _ => Err(anyhow!("Expected array for read response")),
    }
}

pub fn parse_entity_schema_response(response: QuspResponse) -> Result<EntitySchema<Single>> {
    match response {
        QuspResponse::Bulk(bytes) => decode_entity_schema_single(&bytes),
        _ => Err(anyhow!("Expected bulk bytes for EntitySchema")),
    }
}

pub fn parse_complete_entity_schema_response(response: QuspResponse) -> Result<EntitySchema<Complete>> {
    match response {
        QuspResponse::Bulk(bytes) => decode_entity_schema_complete(&bytes),
        _ => Err(anyhow!("Expected bulk bytes for Complete EntitySchema")),
    }
}

pub fn parse_field_schema_response(response: QuspResponse) -> Result<FieldSchema> {
    match response {
        QuspResponse::Bulk(bytes) => decode_field_schema(&bytes),
        _ => Err(anyhow!("Expected bulk bytes for FieldSchema")),
    }
}

pub fn parse_snapshot_response(response: QuspResponse) -> Result<Snapshot> {
    match response {
        QuspResponse::Bulk(bytes) => {
            // TODO: Replace with RESP decoding when Snapshot encoding is implemented
            bincode::deserialize(bytes.as_ref())
                .map_err(|e| anyhow!("Failed to decode Snapshot: {}", e))
        }
        _ => Err(anyhow!("Expected bulk bytes for Snapshot")),
    }
}

pub fn parse_page_result_entity_ids_response(response: QuspResponse) -> Result<PageResult<EntityId>> {
    match response {
        QuspResponse::Array(items) => {
            if items.len() != 3 {
                return Err(anyhow!("PageResult response expects 3 elements"));
            }
            let entity_ids = match &items[0] {
                QuspResponse::Array(id_items) => {
                    let mut ids = Vec::new();
                    for item in id_items {
                        match item {
                            QuspResponse::Integer(i) => ids.push(EntityId(*i as u64)),
                            _ => return Err(anyhow!("Expected integer for EntityId")),
                        }
                    }
                    ids
                }
                _ => return Err(anyhow!("Expected array for entity IDs")),
            };
            let total = match &items[1] {
                QuspResponse::Integer(i) => *i as usize,
                _ => return Err(anyhow!("Expected integer for total")),
            };
            let next_cursor = match &items[2] {
                QuspResponse::Integer(i) => Some(*i as usize),
                QuspResponse::Null => None,
                _ => return Err(anyhow!("Expected integer or null for next_cursor")),
            };
            Ok(PageResult::new(entity_ids, total, next_cursor))
        }
        _ => Err(anyhow!("Expected array for PageResult")),
    }
}

pub fn parse_page_result_entity_types_response(response: QuspResponse) -> Result<PageResult<EntityType>> {
    match response {
        QuspResponse::Array(items) => {
            if items.len() != 3 {
                return Err(anyhow!("PageResult response expects 3 elements"));
            }
            let entity_types = match &items[0] {
                QuspResponse::Array(type_items) => {
                    let mut types = Vec::new();
                    for item in type_items {
                        match item {
                            QuspResponse::Integer(i) => types.push(EntityType(*i as u32)),
                            _ => return Err(anyhow!("Expected integer for EntityType")),
                        }
                    }
                    types
                }
                _ => return Err(anyhow!("Expected array for entity types")),
            };
            let total = match &items[1] {
                QuspResponse::Integer(i) => *i as usize,
                _ => return Err(anyhow!("Expected integer for total")),
            };
            let next_cursor = match &items[2] {
                QuspResponse::Integer(i) => Some(*i as usize),
                QuspResponse::Null => None,
                _ => return Err(anyhow!("Expected integer or null for next_cursor")),
            };
            Ok(PageResult::new(entity_types, total, next_cursor))
        }
        _ => Err(anyhow!("Expected array for PageResult")),
    }
}

pub fn parse_entity_ids_response(response: QuspResponse) -> Result<Vec<EntityId>> {
    match response {
        QuspResponse::Array(items) => {
            let mut entity_ids = Vec::new();
            for item in items {
                match item {
                    QuspResponse::Integer(i) => entity_ids.push(EntityId(i as u64)),
                    _ => return Err(anyhow!("Expected integer for EntityId")),
                }
            }
            Ok(entity_ids)
        }
        _ => Err(anyhow!("Expected array for entity IDs")),
    }
}

pub fn parse_entity_types_response(response: QuspResponse) -> Result<Vec<EntityType>> {
    match response {
        QuspResponse::Array(items) => {
            let mut entity_types = Vec::new();
            for item in items {
                match item {
                    QuspResponse::Integer(i) => entity_types.push(EntityType(i as u32)),
                    _ => return Err(anyhow!("Expected integer for EntityType")),
                }
            }
            Ok(entity_types)
        }
        _ => Err(anyhow!("Expected array for entity types")),
    }
}

// Helper function to convert field path to comma-separated string
pub fn field_path_to_string(field_path: &[FieldType]) -> String {
    field_path
        .iter()
        .map(|ft| ft.0.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

// Helper function to encode optional timestamp
pub fn encode_optional_timestamp(timestamp: Option<Timestamp>) -> String {
    match timestamp {
        Some(ts) => (ts.unix_timestamp_nanos() as u64).to_string(),
        None => "null".to_string(),
    }
}

// Helper function to encode optional entity ID
pub fn encode_optional_entity_id(entity_id: Option<EntityId>) -> String {
    match entity_id {
        Some(id) => id.0.to_string(),
        None => "null".to_string(),
    }
}

// Helper function to encode PushCondition
pub fn encode_push_condition(condition: Option<PushCondition>) -> String {
    match condition {
        Some(PushCondition::Always) => "Always".to_string(),
        Some(PushCondition::Changes) => "Changes".to_string(),
        None => "null".to_string(),
    }
}

// Helper function to encode AdjustBehavior
pub fn encode_adjust_behavior(behavior: Option<AdjustBehavior>) -> String {
    match behavior {
        Some(AdjustBehavior::Set) => "Set".to_string(),
        Some(AdjustBehavior::Add) => "Add".to_string(),
        Some(AdjustBehavior::Subtract) => "Subtract".to_string(),
        None => "null".to_string(),
    }
}

// Export expect_ok function
pub fn expect_ok(response: QuspResponse) -> Result<()> {
    match response {
        QuspResponse::Simple(bytes) => {
            let s = std::str::from_utf8(bytes.as_ref()).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
            if s == "OK" {
                Ok(())
            } else {
                Err(anyhow!("expected OK, got: {}", s))
            }
        }
        QuspResponse::Error(msg) => Err(anyhow!("server error: {}", msg)),
        _ => Err(anyhow!("expected simple string response")),
    }
}
