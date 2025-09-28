use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use std::str;

use crate::{EntityId, EntityType, FieldType, Value, Timestamp, FieldSchema, AdjustBehavior, PageOpts, NotifyConfig, PushCondition, EntitySchema, Single, Complete, PageResult, Snapshot};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const CRLF: &[u8] = b"\r\n";


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
fn encode_value(value: &Value) -> Vec<u8> {
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

// Binary encoding for FieldSchema using bincode for simplicity but efficient binary format
fn encode_field_schema(schema: &FieldSchema) -> Result<Vec<u8>> {
    bincode::serialize(schema).map_err(|e| anyhow!("failed to encode FieldSchema: {}", e))
}

fn decode_field_schema(bytes: &Bytes) -> Result<FieldSchema> {
    bincode::deserialize(bytes.as_ref()).map_err(|e| anyhow!("failed to decode FieldSchema: {}", e))
}

// Binary encoding for PageOpts
fn encode_page_opts(opts: &PageOpts) -> Vec<u8> {
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

// Binary encoding for NotifyConfig
fn encode_notify_config(config: &NotifyConfig) -> Result<Vec<u8>> {
    bincode::serialize(config).map_err(|e| anyhow!("failed to encode NotifyConfig: {}", e))
}

fn decode_notify_config(bytes: &Bytes) -> Result<NotifyConfig> {
    bincode::deserialize(bytes.as_ref()).map_err(|e| anyhow!("failed to decode NotifyConfig: {}", e))
}

// Binary encoding for EntitySchema<Single, String, String>
fn encode_entity_schema_string(schema: &EntitySchema<Single, String, String>) -> Result<Vec<u8>> {
    bincode::serialize(schema).map_err(|e| anyhow!("failed to encode EntitySchema: {}", e))
}

fn decode_entity_schema_string(bytes: &Bytes) -> Result<EntitySchema<Single, String, String>> {
    bincode::deserialize(bytes.as_ref()).map_err(|e| anyhow!("failed to decode EntitySchema: {}", e))
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

fn parse_page_opts(bytes: &Bytes) -> Result<PageOpts> {
    let s = parse_str(bytes)?;
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 2 {
        return Err(anyhow!("PageOpts expects limit,cursor"));
    }
    let limit: usize = parts[0].trim().parse().map_err(|e| anyhow!("invalid limit: {}", e))?;
    let cursor = if parts[1].trim() == "null" {
        None
    } else {
        Some(parts[1].trim().parse().map_err(|e| anyhow!("invalid cursor: {}", e))?)
    };
    Ok(PageOpts::new(limit, cursor))
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

pub fn parse_store_command(cmd: &QuspCommand) -> Result<StoreCommand> {
    let name = cmd.uppercase_name()?;
    match name.as_str() {
        "GET_ENTITY_TYPE" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("GET_ENTITY_TYPE expects 1 argument"));
            }
            let name = parse_str(&cmd.args[0])?;
            Ok(StoreCommand::GetEntityType { name })
        }
        "RESOLVE_ENTITY_TYPE" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("RESOLVE_ENTITY_TYPE expects 1 argument"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            Ok(StoreCommand::ResolveEntityType { entity_type })
        }
        "GET_FIELD_TYPE" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("GET_FIELD_TYPE expects 1 argument"));
            }
            let name = parse_str(&cmd.args[0])?;
            Ok(StoreCommand::GetFieldType { name })
        }
        "RESOLVE_FIELD_TYPE" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("RESOLVE_FIELD_TYPE expects 1 argument"));
            }
            let field_type = parse_field_type(&cmd.args[0])?;
            Ok(StoreCommand::ResolveFieldType { field_type })
        }
        "GET_ENTITY_SCHEMA" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("GET_ENTITY_SCHEMA expects 1 argument"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            Ok(StoreCommand::GetEntitySchema { entity_type })
        }
        "GET_COMPLETE_ENTITY_SCHEMA" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("GET_COMPLETE_ENTITY_SCHEMA expects 1 argument"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            Ok(StoreCommand::GetCompleteEntitySchema { entity_type })
        }
        "GET_FIELD_SCHEMA" => {
            if cmd.args.len() != 2 {
                return Err(anyhow!("GET_FIELD_SCHEMA expects 2 arguments"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            let field_type = parse_field_type(&cmd.args[1])?;
            Ok(StoreCommand::GetFieldSchema { entity_type, field_type })
        }
        "SET_FIELD_SCHEMA" => {
            if cmd.args.len() != 3 {
                return Err(anyhow!("SET_FIELD_SCHEMA expects 3 arguments"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            let field_type = parse_field_type(&cmd.args[1])?;
            let schema = decode_field_schema(&cmd.args[2])?;
            Ok(StoreCommand::SetFieldSchema { entity_type, field_type, schema })
        }
        "ENTITY_EXISTS" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("ENTITY_EXISTS expects 1 argument"));
            }
            let entity_id = parse_entity_id(&cmd.args[0])?;
            Ok(StoreCommand::EntityExists { entity_id })
        }
        "FIELD_EXISTS" => {
            if cmd.args.len() != 2 {
                return Err(anyhow!("FIELD_EXISTS expects 2 arguments"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            let field_type = parse_field_type(&cmd.args[1])?;
            Ok(StoreCommand::FieldExists { entity_type, field_type })
        }
        "RESOLVE_INDIRECTION" => {
            if cmd.args.len() != 2 {
                return Err(anyhow!("RESOLVE_INDIRECTION expects 2 arguments"));
            }
            let entity_id = parse_entity_id(&cmd.args[0])?;
            let field_path = parse_field_path(&cmd.args[1])?;
            Ok(StoreCommand::ResolveIndirection { entity_id, field_path })
        }
        "READ" => {
            if cmd.args.len() != 2 {
                return Err(anyhow!("READ expects 2 arguments"));
            }
            let entity_id = parse_entity_id(&cmd.args[0])?;
            let field_path = parse_field_path(&cmd.args[1])?;
            Ok(StoreCommand::Read { entity_id, field_path })
        }
        "WRITE" => {
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
        "CREATE_ENTITY" => {
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
        "DELETE_ENTITY" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("DELETE_ENTITY expects 1 argument"));
            }
            let entity_id = parse_entity_id(&cmd.args[0])?;
            Ok(StoreCommand::DeleteEntity { entity_id })
        }
        "UPDATE_SCHEMA" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("UPDATE_SCHEMA expects 1 argument"));
            }
            let schema = decode_entity_schema_string(&cmd.args[0])?;
            Ok(StoreCommand::UpdateSchema { schema })
        }
        "TAKE_SNAPSHOT" => {
            if cmd.args.len() != 0 {
                return Err(anyhow!("TAKE_SNAPSHOT expects no arguments"));
            }
            Ok(StoreCommand::TakeSnapshot)
        }
        "FIND_ENTITIES_PAGINATED" => {
            if cmd.args.len() < 1 || cmd.args.len() > 3 {
                return Err(anyhow!("FIND_ENTITIES_PAGINATED expects 1-3 arguments"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            let page_opts = if cmd.args.len() > 1 {
                let opts_str = parse_str(&cmd.args[1])?;
                if opts_str == "null" {
                    None
                } else {
                    Some(decode_page_opts(&cmd.args[1])?)
                }
            } else {
                None
            };
            let filter = if cmd.args.len() > 2 {
                let filter_str = parse_str(&cmd.args[2])?;
                if filter_str == "null" {
                    None
                } else {
                    Some(filter_str.to_string())
                }
            } else {
                None
            };
            Ok(StoreCommand::FindEntitiesPaginated { entity_type, page_opts, filter })
        }
        "FIND_ENTITIES_EXACT" => {
            if cmd.args.len() < 1 || cmd.args.len() > 3 {
                return Err(anyhow!("FIND_ENTITIES_EXACT expects 1-3 arguments"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            let page_opts = if cmd.args.len() > 1 {
                let opts_str = parse_str(&cmd.args[1])?;
                if opts_str == "null" {
                    None
                } else {
                    Some(decode_page_opts(&cmd.args[1])?)
                }
            } else {
                None
            };
            let filter = if cmd.args.len() > 2 {
                let filter_str = parse_str(&cmd.args[2])?;
                if filter_str == "null" {
                    None
                } else {
                    Some(filter_str.to_string())
                }
            } else {
                None
            };
            Ok(StoreCommand::FindEntitiesExact { entity_type, page_opts, filter })
        }
        "FIND_ENTITIES" => {
            if cmd.args.len() < 1 || cmd.args.len() > 2 {
                return Err(anyhow!("FIND_ENTITIES expects 1-2 arguments"));
            }
            let entity_type = parse_entity_type(&cmd.args[0])?;
            let filter = if cmd.args.len() > 1 {
                let filter_str = parse_str(&cmd.args[1])?;
                if filter_str == "null" {
                    None
                } else {
                    Some(filter_str.to_string())
                }
            } else {
                None
            };
            Ok(StoreCommand::FindEntities { entity_type, filter })
        }
        "GET_ENTITY_TYPES" => {
            if cmd.args.len() != 0 {
                return Err(anyhow!("GET_ENTITY_TYPES expects no arguments"));
            }
            Ok(StoreCommand::GetEntityTypes)
        }
        "GET_ENTITY_TYPES_PAGINATED" => {
            if cmd.args.len() > 1 {
                return Err(anyhow!("GET_ENTITY_TYPES_PAGINATED expects 0-1 arguments"));
            }
            let page_opts = if cmd.args.len() > 0 {
                let opts_str = parse_str(&cmd.args[0])?;
                if opts_str == "null" {
                    None
                } else {
                    Some(decode_page_opts(&cmd.args[0])?)
                }
            } else {
                None
            };
            Ok(StoreCommand::GetEntityTypesPaginated { page_opts })
        }
        "REGISTER_NOTIFICATION" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("REGISTER_NOTIFICATION expects 1 argument"));
            }
            let config = decode_notify_config(&cmd.args[0])?;
            Ok(StoreCommand::RegisterNotification { config })
        }
        "UNREGISTER_NOTIFICATION" => {
            if cmd.args.len() != 1 {
                return Err(anyhow!("UNREGISTER_NOTIFICATION expects 1 argument"));
            }
            let config = decode_notify_config(&cmd.args[0])?;
            Ok(StoreCommand::UnregisterNotification { config })
        }
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

fn parse_entity_id_str(s: &str) -> Result<EntityId> {
    s.trim().parse().map_err(|e| anyhow!("invalid entity id: {}", e)).map(EntityId)
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
    let encoded = bincode::serialize(schema).map_err(|e| anyhow!("failed to encode EntitySchema: {}", e))?;
    Ok(QuspResponse::Bulk(Bytes::copy_from_slice(&encoded)))
}

pub fn encode_complete_entity_schema_response(schema: &EntitySchema<Complete>) -> Result<QuspResponse> {
    let encoded = bincode::serialize(schema).map_err(|e| anyhow!("failed to encode Complete EntitySchema: {}", e))?;
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




