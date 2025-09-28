use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use std::str;

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




