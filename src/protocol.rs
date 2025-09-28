use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use std::str;

use crate::{EntityId, EntityType, FieldType, Value, Timestamp, FieldSchema, AdjustBehavior, PageOpts, NotifyConfig, PushCondition, EntitySchema, Single, Complete, PageResult, Snapshot};
use crate::data::StorageScope;

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const CRLF: &[u8] = b"\r\n";

/// Trait for efficient RESP encoding with pre-allocated buffers
pub trait RespEncode {
    /// Encode to a pre-allocated buffer, returning the number of bytes written
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize;
    
    /// Estimate the buffer size needed for encoding (for optimization)
    fn encoded_size_hint(&self) -> usize {
        64 // Default conservative estimate
    }
    
    /// Convenience method for one-shot encoding
    #[inline]
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size_hint());
        self.encode_to(&mut buf);
        buf
    }
}

/// Trait for efficient RESP decoding from owned data
pub trait RespDecode: Sized {
    /// Decode from bytes with detailed error context
    fn decode_from(bytes: &Bytes) -> Result<Self>;
    
    /// Try to decode without consuming the bytes (peek operation)
    fn try_decode_from(bytes: &Bytes) -> Result<Option<Self>> {
        match Self::decode_from(bytes) {
            Ok(value) => Ok(Some(value)),
            Err(_) => Ok(None), // Convert errors to None for optional parsing
        }
    }
}

/// Trait for true zero-copy deserialization from RESP frames
/// This trait provides views into the original buffer without any allocations
pub trait RespView<'a>: Sized {
    /// Create a zero-copy view from the buffer
    fn view_from(bytes: &'a [u8]) -> Result<Self>;
    
    /// Get the number of bytes consumed by this view
    fn consumed_bytes(&self) -> usize;
}

/// Zero-copy frame reference that maintains a view into the original buffer
/// This struct provides true zero-copy access to RESP frame data
#[derive(Debug, Clone)]
pub struct FrameRef<'a> {
    /// Reference to the original buffer containing the frame data
    buffer: &'a [u8],
    /// Start position of the frame's content (after type marker and length)
    content_start: usize,
    /// Length of the frame's content
    content_len: usize,
    /// Total frame size including headers and terminators
    total_len: usize,
    /// Frame type with embedded metadata
    pub frame_type: FrameType,
}

/// Iterator over array elements in a FrameRef
pub struct FrameRefIterator<'a> {
    buffer: &'a [u8],
    position: usize,
    remaining: usize,
}

impl<'a> Iterator for FrameRefIterator<'a> {
    type Item = Result<FrameRef<'a>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        
        match parse_frame_ref_at(self.buffer, self.position) {
            Ok((frame_ref, next_pos)) => {
                self.position = next_pos;
                self.remaining -= 1;
                Some(Ok(frame_ref))
            }
            Err(e) => Some(Err(e)),
        }
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a> ExactSizeIterator for FrameRefIterator<'a> {
    fn len(&self) -> usize {
        self.remaining
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    /// Simple string: +<string>\r\n
    Simple,
    /// Error string: -<string>\r\n
    Error,
    /// Integer: :<number>\r\n
    Integer(i64),
    /// Bulk string: $<length>\r\n<data>\r\n
    Bulk { len: usize },
    /// Null bulk string: $-1\r\n
    Null,
    /// Array: *<count>\r\n...
    Array { count: usize },
}

impl FrameType {
    /// Check if this frame type represents a string-like value
    #[inline]
    pub fn is_string_like(&self) -> bool {
        matches!(self, FrameType::Simple | FrameType::Error | FrameType::Bulk { .. })
    }
    
    /// Check if this frame can contain child elements
    #[inline]
    pub fn is_container(&self) -> bool {
        matches!(self, FrameType::Array { .. })
    }
    
    /// Get the expected data length for this frame type
    #[inline]
    pub fn data_len(&self) -> Option<usize> {
        match self {
            FrameType::Bulk { len } => Some(*len),
            FrameType::Array { count } => Some(*count),
            _ => None,
        }
    }
}

impl<'a> FrameRef<'a> {
    /// Create a new frame reference with content bounds
    #[inline]
    pub fn new(
        buffer: &'a [u8], 
        content_start: usize, 
        content_len: usize, 
        total_len: usize,
        frame_type: FrameType
    ) -> Self {
        Self { 
            buffer, 
            content_start, 
            content_len, 
            total_len,
            frame_type 
        }
    }
    
    /// Get the raw content bytes without copying (true zero-copy)
    #[inline]
    pub fn content_bytes(&self) -> &'a [u8] {
        &self.buffer[self.content_start..self.content_start + self.content_len]
    }
    
    /// Get the entire frame bytes including headers
    #[inline]
    pub fn frame_bytes(&self) -> &'a [u8] {
        let start = self.content_start.saturating_sub(16); // Rough estimate for headers
        &self.buffer[start..self.content_start + self.total_len]
    }
    
    /// Get frame content as a string slice (zero-copy)
    #[inline]
    pub fn as_str(&self) -> Result<&'a str> {
        if !self.frame_type.is_string_like() {
            return Err(anyhow!("frame type {:?} is not string-like", self.frame_type));
        }
        
        std::str::from_utf8(self.content_bytes())
            .map_err(|e| anyhow!("invalid UTF-8 in frame content: {}", e))
    }
    
    /// Get the frame as an integer (for Integer frames)
    #[inline]
    pub fn as_integer(&self) -> Result<i64> {
        match self.frame_type {
            FrameType::Integer(value) => Ok(value),
            _ => Err(anyhow!("frame type {:?} is not an integer", self.frame_type)),
        }
    }
    
    /// Check if this frame represents a null value
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self.frame_type, FrameType::Null)
    }
    
    /// Get the array length without parsing elements
    #[inline]
    pub fn array_len(&self) -> Result<usize> {
        match self.frame_type {
            FrameType::Array { count } => Ok(count),
            _ => Err(anyhow!("frame type {:?} is not an array", self.frame_type)),
        }
    }
    
    /// Get an iterator over array elements (zero-copy)
    pub fn iter_array(&self) -> Result<FrameRefIterator<'a>> {
        let count = self.array_len()?;
        Ok(FrameRefIterator {
            buffer: self.buffer,
            position: self.content_start,
            remaining: count,
        })
    }
    
    /// Get array elements as a vector (convenience method)
    pub fn collect_array(&self) -> Result<Vec<FrameRef<'a>>> {
        self.iter_array()?.collect::<Result<Vec<_>, _>>()
    }
    

    /// Try to get a specific array element by index (zero-copy)
    pub fn array_element(&self, index: usize) -> Result<FrameRef<'a>> {
        let count = self.array_len()?;
        if index >= count {
            return Err(anyhow!("array index {} out of bounds (len: {})", index, count));
        }
        
        let mut iter = self.iter_array()?;
        iter.nth(index)
            .ok_or_else(|| anyhow!("failed to get array element at index {}", index))?
    }
    
    /// Get the total size of this frame including headers and terminators
    #[inline]
    pub fn total_size(&self) -> usize {
        self.total_len
    }
    
    /// Get the content size of this frame
    #[inline]
    pub fn content_size(&self) -> usize {
        self.content_len
    }
}

/// Owned QUSP command with efficient argument access
#[derive(Debug, Clone)]
pub struct QuspCommand {
    pub name: Bytes,
    pub args: Vec<Bytes>,
}

impl QuspCommand {
    /// Create a new command with name and arguments
    #[inline]
    pub fn new<N: Into<Bytes>>(name: N, args: Vec<Bytes>) -> Self {
        Self {
            name: name.into(),
            args,
        }
    }
    
    /// Get the command name as an uppercase string
    #[inline]
    pub fn name_uppercase(&self) -> Result<String> {
        std::str::from_utf8(&self.name)
            .map(|s| s.to_ascii_uppercase())
            .map_err(|e| anyhow!("invalid UTF-8 in command name: {}", e))
    }
    
    /// Get the number of arguments
    #[inline]
    pub fn arg_count(&self) -> usize {
        self.args.len()
    }
    
    /// Check if the command has the expected number of arguments
    #[inline]
    pub fn has_args(&self, count: usize) -> bool {
        self.args.len() == count
    }
    
    /// Check if the command has arguments in the specified range
    #[inline]
    pub fn has_args_range(&self, min: usize, max: usize) -> bool {
        let count = self.args.len();
        count >= min && count <= max
    }
    
    /// Get an argument as bytes
    #[inline]
    pub fn arg_bytes(&self, index: usize) -> Option<&[u8]> {
        self.args.get(index).map(|b| b.as_ref())
    }
    
    /// Get an argument as a string
    #[inline]
    pub fn arg_str(&self, index: usize) -> Result<&str> {
        match self.args.get(index) {
            Some(bytes) => std::str::from_utf8(bytes)
                .map_err(|e| anyhow!("invalid UTF-8 in argument {}: {}", index, e)),
            None => Err(anyhow!("argument {} not found", index)),
        }
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

/// Parse a frame reference at a specific position without copying data
fn parse_frame_ref_at(buffer: &[u8], start: usize) -> Result<(FrameRef, usize)> {
    parse_frame_ref_impl(buffer, start)
}

/// Parse the root frame as a zero-copy reference from bytes
pub fn parse_root_frame_ref(bytes: &[u8]) -> Result<FrameRef> {
    let (frame_ref, consumed) = parse_frame_ref_impl(bytes, 0)?;
    if consumed != bytes.len() {
        return Err(anyhow!("extra {} bytes after QUSP frame", bytes.len() - consumed));
    }
    Ok(frame_ref)
}

/// Internal implementation of frame reference parsing
fn parse_frame_ref_impl(buffer: &[u8], start: usize) -> Result<(FrameRef, usize)> {
    if start >= buffer.len() {
        return Err(anyhow!("unexpected end of RESP frame at position {}", start));
    }

    let type_byte = buffer[start];
    match type_byte {
        b'+' => {
            // Simple string: +<content>\r\n
            match read_line_end(buffer, start + 1) {
                ParseStatus::Complete(content_end) => {
                    let content_start = start + 1;
                    let content_len = content_end - content_start;
                    let total_len = content_end + 2 - start; // Include CRLF
                    let frame_ref = FrameRef::new(
                        buffer,
                        content_start,
                        content_len,
                        total_len,
                        FrameType::Simple
                    );
                    Ok((frame_ref, content_end + 2))
                }
                ParseStatus::Incomplete => Err(anyhow!("unterminated simple string")),
            }
        },
        b'-' => {
            // Error string: -<content>\r\n
            match read_line_end(buffer, start + 1) {
                ParseStatus::Complete(content_end) => {
                    let content_start = start + 1;
                    let content_len = content_end - content_start;
                    let total_len = content_end + 2 - start;
                    let frame_ref = FrameRef::new(
                        buffer,
                        content_start,
                        content_len,
                        total_len,
                        FrameType::Error
                    );
                    Ok((frame_ref, content_end + 2))
                }
                ParseStatus::Incomplete => Err(anyhow!("unterminated error string")),
            }
        },
        b':' => {
            // Integer: :<number>\r\n
            match parse_number_line(buffer, start + 1)? {
                ParseStatus::Complete((value, end_pos)) => {
                    let total_len = end_pos - start;
                    let frame_ref = FrameRef::new(
                        buffer,
                        start + 1, // Content starts after ':'
                        0,         // Integer content is embedded in type
                        total_len,
                        FrameType::Integer(value)
                    );
                    Ok((frame_ref, end_pos))
                }
                ParseStatus::Incomplete => Err(anyhow!("unterminated integer")),
            }
        },
        b'$' => {
            // Bulk string: $<length>\r\n<data>\r\n or $-1\r\n for null
            match parse_number_line(buffer, start + 1)? {
                ParseStatus::Complete((len_val, header_end)) => {
                    if len_val < -1 {
                        return Err(anyhow!("invalid bulk string length: {}", len_val));
                    }
                    if len_val == -1 {
                        // Null bulk string
                        let total_len = header_end - start;
                        let frame_ref = FrameRef::new(
                            buffer,
                            header_end, // No content for null
                            0,
                            total_len,
                            FrameType::Null
                        );
                        return Ok((frame_ref, header_end));
                    }
                    
                    let data_len = len_val as usize;
                    let content_start = header_end;
                    let content_end = content_start + data_len;
                    
                    if content_end + 2 > buffer.len() {
                        return Err(anyhow!("truncated bulk string: expected {} bytes + CRLF", data_len));
                    }
                    
                    if &buffer[content_end..content_end + 2] != CRLF {
                        return Err(anyhow!("bulk string missing CRLF terminator"));
                    }
                    
                    let total_len = content_end + 2 - start;
                    let frame_ref = FrameRef::new(
                        buffer,
                        content_start,
                        data_len,
                        total_len,
                        FrameType::Bulk { len: data_len }
                    );
                    Ok((frame_ref, content_end + 2))
                }
                ParseStatus::Incomplete => Err(anyhow!("unterminated bulk string length")),
            }
        },
        b'*' => {
            // Array: *<count>\r\n<elements...> or *-1\r\n for null
            match parse_number_line(buffer, start + 1)? {
                ParseStatus::Complete((count_val, header_end)) => {
                    if count_val < -1 {
                        return Err(anyhow!("invalid array length: {}", count_val));
                    }
                    if count_val == -1 {
                        // Null array
                        let total_len = header_end - start;
                        let frame_ref = FrameRef::new(
                            buffer,
                            header_end,
                            0,
                            total_len,
                            FrameType::Null
                        );
                        return Ok((frame_ref, header_end));
                    }
                    
                    let count = count_val as usize;
                    let content_start = header_end;
                    let mut pos = content_start;
                    
                    // Skip over all elements to calculate total frame size
                    for i in 0..count {
                        match parse_frame_ref_impl(buffer, pos) {
                            Ok((_, next_pos)) => pos = next_pos,
                            Err(e) => return Err(anyhow!("failed to parse array element {}: {}", i, e)),
                        }
                    }
                    
                    let total_len = pos - start;
                    let frame_ref = FrameRef::new(
                        buffer,
                        content_start,
                        pos - content_start,
                        total_len,
                        FrameType::Array { count }
                    );
                    Ok((frame_ref, pos))
                }
                ParseStatus::Incomplete => Err(anyhow!("unterminated array length")),
            }
        },
        _ => Err(anyhow!("unsupported RESP type byte: 0x{:02x}", type_byte)),
    }
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

/// Efficient RESP encoder with reduced allocations and improved performance
#[derive(Debug)]
pub struct RespEncoder {
    buffer: Vec<u8>,
}

impl RespEncoder {
    /// Create a new encoder with capacity hint
    #[inline]
    pub fn new() -> Self {
        Self { buffer: Vec::with_capacity(1024) }
    }
    
    /// Create a new encoder with specified capacity
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self { buffer: Vec::with_capacity(capacity) }
    }
    
    /// Write array header: *<count>\r\n
    #[inline]
    pub fn write_array_header(&mut self, count: usize) -> &mut Self {
        self.buffer.push(b'*');
        self.write_decimal(count as u64);
        self.buffer.extend_from_slice(CRLF);
        self
    }
    
    /// Write bulk string: $<len>\r\n<data>\r\n
    #[inline]
    pub fn write_bulk_string(&mut self, data: &[u8]) -> &mut Self {
        self.buffer.push(b'$');
        self.write_decimal(data.len() as u64);
        self.buffer.extend_from_slice(CRLF);
        self.buffer.extend_from_slice(data);
        self.buffer.extend_from_slice(CRLF);
        self
    }
    
    /// Write simple string: +<data>\r\n
    #[inline]
    pub fn write_simple_string(&mut self, data: &[u8]) -> &mut Self {
        self.buffer.push(b'+');
        self.buffer.extend_from_slice(data);
        self.buffer.extend_from_slice(CRLF);
        self
    }
    
    /// Write error string: -<data>\r\n
    #[inline]
    pub fn write_error(&mut self, data: &[u8]) -> &mut Self {
        self.buffer.push(b'-');
        self.buffer.extend_from_slice(data);
        self.buffer.extend_from_slice(CRLF);
        self
    }
    
    /// Write integer: :<value>\r\n
    #[inline]
    pub fn write_integer(&mut self, value: i64) -> &mut Self {
        self.buffer.push(b':');
        if value < 0 {
            self.buffer.push(b'-');
            self.write_decimal((-value) as u64);
        } else {
            self.write_decimal(value as u64);
        }
        self.buffer.extend_from_slice(CRLF);
        self
    }
    
    /// Write null value: $-1\r\n
    #[inline]
    pub fn write_null(&mut self) -> &mut Self {
        self.buffer.extend_from_slice(b"$-1\r\n");
        self
    }
    
    /// Write decimal number to buffer
    #[inline]
    fn write_decimal(&mut self, mut value: u64) {
        if value == 0 {
            self.buffer.push(b'0');
            return;
        }
        
        let start_len = self.buffer.len();
        while value > 0 {
            self.buffer.push(b'0' + (value % 10) as u8);
            value /= 10;
        }
        
        // Reverse the digits
        let end_len = self.buffer.len();
        self.buffer[start_len..end_len].reverse();
    }
    
    /// Get the encoded bytes without consuming the encoder
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }
    
    /// Get the encoded bytes and reset the buffer
    #[inline]
    pub fn take_bytes(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }
    
    /// Clear the buffer for reuse
    #[inline]
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
    
    /// Get the current buffer size
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    
    /// Check if the buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

impl Default for RespEncoder {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Convenient encoding functions using the new encoder

/// Encode a QUSP command with improved efficiency
pub fn encode_command(command: &QuspCommand) -> Vec<u8> {
    let estimated_capacity = 32 + command.name.len() + 
        command.args.iter().map(|arg| arg.len() + 8).sum::<usize>();
    
    let mut encoder = RespEncoder::with_capacity(estimated_capacity);
    encoder.write_array_header(1 + command.args.len());
    encoder.write_bulk_string(command.name.as_ref());
    
    for arg in &command.args {
        encoder.write_bulk_string(arg.as_ref());
    }
    
    encoder.take_bytes()
}

/// Encode a simple OK response
#[inline]
pub fn encode_ok() -> Vec<u8> {
    let mut encoder = RespEncoder::with_capacity(5);
    encoder.write_simple_string(b"OK");
    encoder.take_bytes()
}

/// Encode a simple string response
#[inline]
pub fn encode_simple_string(message: &str) -> Vec<u8> {
    let mut encoder = RespEncoder::with_capacity(message.len() + 4);
    encoder.write_simple_string(message.as_bytes());
    encoder.take_bytes()
}

/// Encode bulk bytes
#[inline]
pub fn encode_bulk_bytes(data: &[u8]) -> Vec<u8> {
    let mut encoder = RespEncoder::with_capacity(data.len() + 8);
    encoder.write_bulk_string(data);
    encoder.take_bytes()
}

/// Encode an integer value
#[inline]
pub fn encode_integer(value: i64) -> Vec<u8> {
    let mut encoder = RespEncoder::with_capacity(32);
    encoder.write_integer(value);
    encoder.take_bytes()
}

/// Encode a null value
#[inline]
pub fn encode_null() -> Vec<u8> {
    let mut encoder = RespEncoder::with_capacity(5);
    encoder.write_null();
    encoder.take_bytes()
}

/// Encode an error message
#[inline]
pub fn encode_error(message: &str) -> Vec<u8> {
    let mut encoder = RespEncoder::with_capacity(message.len() + 4);
    encoder.write_error(message.as_bytes());
    encoder.take_bytes()
}

/// Encode a QUSP response
pub fn encode_response(response: &QuspResponse) -> Vec<u8> {
    let mut encoder = RespEncoder::with_capacity(response.encoded_size_hint());
    response.encode_to(&mut encoder.buffer);
    encoder.take_bytes()
}

// Trait implementations for basic types using the new encoder
impl RespEncode for i64 {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        let mut encoder = RespEncoder { buffer: std::mem::take(buf) };
        encoder.write_integer(*self);
        *buf = encoder.buffer;
        buf.len() - start_len
    }
    
    #[inline]
    fn encoded_size_hint(&self) -> usize {
        // ":" + digits + "\r\n"
        if *self == 0 {
            4
        } else {
            3 + (self.abs() as f64).log10() as usize + 1 + if *self < 0 { 1 } else { 0 }
        }
    }
}

impl RespEncode for String {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        let mut encoder = RespEncoder { buffer: std::mem::take(buf) };
        encoder.write_bulk_string(self.as_bytes());
        *buf = encoder.buffer;
        buf.len() - start_len
    }
    
    #[inline]
    fn encoded_size_hint(&self) -> usize {
        // "$" + length_digits + "\r\n" + data + "\r\n"
        let len_digits = if self.is_empty() { 1 } else { (self.len() as f64).log10() as usize + 1 };
        1 + len_digits + 2 + self.len() + 2
    }
}

impl RespEncode for &str {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        let mut encoder = RespEncoder { buffer: std::mem::take(buf) };
        encoder.write_bulk_string(self.as_bytes());
        *buf = encoder.buffer;
        buf.len() - start_len
    }
    
    #[inline]
    fn encoded_size_hint(&self) -> usize {
        let len_digits = if self.is_empty() { 1 } else { (self.len() as f64).log10() as usize + 1 };
        1 + len_digits + 2 + self.len() + 2
    }
}

impl RespEncode for &[u8] {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        let mut encoder = RespEncoder { buffer: std::mem::take(buf) };
        encoder.write_bulk_string(self);
        *buf = encoder.buffer;
        buf.len() - start_len
    }
    
    #[inline]
    fn encoded_size_hint(&self) -> usize {
        let len_digits = if self.is_empty() { 1 } else { (self.len() as f64).log10() as usize + 1 };
        1 + len_digits + 2 + self.len() + 2
    }
}

impl RespEncode for Bytes {
    #[inline]
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        let mut encoder = RespEncoder { buffer: std::mem::take(buf) };
        encoder.write_bulk_string(self.as_ref());
        *buf = encoder.buffer;
        buf.len() - start_len
    }
    
    #[inline]
    fn encoded_size_hint(&self) -> usize {
        let len_digits = if self.is_empty() { 1 } else { (self.len() as f64).log10() as usize + 1 };
        1 + len_digits + 2 + self.len() + 2
    }
}

impl<T: RespEncode> RespEncode for Vec<T> {
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        let mut encoder = RespEncoder { buffer: std::mem::take(buf) };
        encoder.write_array_header(self.len());
        
        let mut _total_encoded = 0;
        for item in self {
            _total_encoded += item.encode_to(&mut encoder.buffer);
        }
        
        *buf = encoder.buffer;
        buf.len() - start_len
    }
    
    fn encoded_size_hint(&self) -> usize {
        let array_header_size = 1 + (self.len() as f64).log10() as usize + 1 + 2; // "*" + digits + "\r\n"
        let items_size: usize = self.iter().map(|item| item.encoded_size_hint()).sum();
        array_header_size + items_size
    }
}

impl<T: RespEncode> RespEncode for Option<T> {
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            Some(value) => value.encode_to(buf),
            None => {
                let start_len = buf.len();
                let mut encoder = RespEncoder { buffer: std::mem::take(buf) };
                encoder.write_null();
                *buf = encoder.buffer;
                buf.len() - start_len
            }
        }
    }
    
    fn encoded_size_hint(&self) -> usize {
        match self {
            Some(value) => value.encoded_size_hint(),
            None => 5, // "$-1\r\n"
        }
    }
}

impl RespEncode for QuspResponse {
    fn encode_to(&self, buf: &mut Vec<u8>) -> usize {
        let start_len = buf.len();
        let mut encoder = RespEncoder { buffer: std::mem::take(buf) };
        
        match self {
            QuspResponse::Simple(bytes) => {
                encoder.write_simple_string(bytes.as_ref());
            }
            QuspResponse::Bulk(bytes) => {
                encoder.write_bulk_string(bytes.as_ref());
            }
            QuspResponse::Integer(i) => {
                encoder.write_integer(*i);
            }
            QuspResponse::Null => {
                encoder.write_null();
            }
            QuspResponse::Array(arr) => {
                encoder.write_array_header(arr.len());
                for response in arr {
                    response.encode_to(&mut encoder.buffer);
                }
            }
            QuspResponse::Error(s) => {
                encoder.write_error(s.as_bytes());
            }
        }
        
        *buf = encoder.buffer;
        buf.len() - start_len
    }
    
    fn encoded_size_hint(&self) -> usize {
        match self {
            QuspResponse::Simple(bytes) => 1 + bytes.len() + 2, // "+" + data + "\r\n"
            QuspResponse::Bulk(bytes) => {
                let len_digits = if bytes.is_empty() { 1 } else { (bytes.len() as f64).log10() as usize + 1 };
                1 + len_digits + 2 + bytes.len() + 2
            }
            QuspResponse::Integer(i) => {
                if *i == 0 {
                    4
                } else {
                    3 + (i.abs() as f64).log10() as usize + 1 + if *i < 0 { 1 } else { 0 }
                }
            }
            QuspResponse::Null => 5, // "$-1\r\n"
            QuspResponse::Array(arr) => {
                let header_size = 1 + (arr.len() as f64).log10() as usize + 1 + 2;
                let items_size: usize = arr.iter().map(|r| r.encoded_size_hint()).sum();
                header_size + items_size
            }
            QuspResponse::Error(s) => 1 + s.len() + 2, // "-" + data + "\r\n"
        }
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

// Zero-copy view implementations
impl<'a> RespView<'a> for &'a str {
    fn view_from(bytes: &'a [u8]) -> Result<Self> {
        std::str::from_utf8(bytes).map_err(|e| anyhow!("invalid UTF-8: {}", e))
    }
    
    fn consumed_bytes(&self) -> usize {
        self.len()
    }
}

impl<'a> RespView<'a> for &'a [u8] {
    fn view_from(bytes: &'a [u8]) -> Result<Self> {
        Ok(bytes)
    }
    
    fn consumed_bytes(&self) -> usize {
        self.len()
    }
}

impl<'a> RespView<'a> for u32 {
    fn view_from(bytes: &'a [u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
        s.parse().map_err(|e| anyhow!("invalid u32: {}", e))
    }
    
    fn consumed_bytes(&self) -> usize {
        self.to_string().len()
    }
}

impl<'a> RespView<'a> for u64 {
    fn view_from(bytes: &'a [u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
        s.parse().map_err(|e| anyhow!("invalid u64: {}", e))
    }
    
    fn consumed_bytes(&self) -> usize {
        self.to_string().len()
    }
}

impl<'a> RespView<'a> for EntityId {
    fn view_from(bytes: &'a [u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
        let val: u64 = s.parse().map_err(|e| anyhow!("invalid entity ID: {}", e))?;
        Ok(EntityId(val))
    }
    
    fn consumed_bytes(&self) -> usize {
        self.0.to_string().len()
    }
}

impl<'a> RespView<'a> for EntityType {
    fn view_from(bytes: &'a [u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
        let val: u32 = s.parse().map_err(|e| anyhow!("invalid entity type: {}", e))?;
        Ok(EntityType(val))
    }
    
    fn consumed_bytes(&self) -> usize {
        self.0.to_string().len()
    }
}

impl<'a> RespView<'a> for FieldType {
    fn view_from(bytes: &'a [u8]) -> Result<Self> {
        let s = std::str::from_utf8(bytes).map_err(|e| anyhow!("invalid UTF-8: {}", e))?;
        let val: u64 = s.parse().map_err(|e| anyhow!("invalid field type: {}", e))?;
        Ok(FieldType(val))
    }
    
    fn consumed_bytes(&self) -> usize {
        self.0.to_string().len()
    }
}

/// Zero-copy command reference that maintains views into the original buffer
#[derive(Debug, Clone)]
pub struct QuspCommandRef<'a> {
    pub name: &'a str,
    args_frame: FrameRef<'a>,
}

impl<'a> QuspCommandRef<'a> {
    /// Create a new zero-copy command reference from a frame
    pub fn from_frame_ref(frame_ref: FrameRef<'a>) -> Result<Self> {
        let mut elements = frame_ref.iter_array()?;
        
        // First element should be the command name
        let name_frame = elements.next()
            .ok_or_else(|| anyhow!("QUSP command missing name"))??;
        let name = name_frame.as_str()?;
        
        Ok(Self { 
            name,
            args_frame: frame_ref,
        })
    }
    
    /// Get the uppercase command name (allocates)
    #[inline]
    pub fn name_uppercase(&self) -> String {
        self.name.to_ascii_uppercase()
    }
    
    /// Get the number of arguments (excluding command name)
    #[inline]
    pub fn arg_count(&self) -> usize {
        self.args_frame.array_len().unwrap_or(1).saturating_sub(1)
    }
    
    /// Check if the command has the expected number of arguments
    #[inline]
    pub fn has_args(&self, count: usize) -> bool {
        self.arg_count() == count
    }
    
    /// Check if the command has arguments in the specified range
    #[inline]
    pub fn has_args_range(&self, min: usize, max: usize) -> bool {
        let count = self.arg_count();
        count >= min && count <= max
    }
    
    /// Get a specific argument as a string slice (zero-copy)
    pub fn arg_str(&self, index: usize) -> Result<&'a str> {
        // Add 1 to skip the command name
        let frame = self.args_frame.array_element(index + 1)?;
        frame.as_str()
    }
    
    /// Get a specific argument as bytes (zero-copy)
    pub fn arg_bytes(&self, index: usize) -> Result<&'a [u8]> {
        // Add 1 to skip the command name
        let frame = self.args_frame.array_element(index + 1)?;
        Ok(frame.content_bytes())
    }
    
    /// Get an iterator over all arguments (excluding command name)
    pub fn iter_args(&self) -> Result<impl Iterator<Item = Result<FrameRef<'a>>> + 'a> {
        let mut iter = self.args_frame.iter_array()?;
        // Skip the first element (command name)
        iter.next();
        Ok(iter)
    }
}

/// Generic response builders to reduce code duplication
pub struct ResponseBuilder;

impl ResponseBuilder {
    /// Create a simple integer response from any integer-like type
    #[inline]
    pub fn integer<T>(value: T) -> QuspResponse 
    where 
        T: Into<i64>,
    {
        QuspResponse::Integer(value.into())
    }
    
    /// Create a bulk string response
    #[inline]
    pub fn bulk_string(value: &str) -> QuspResponse {
        QuspResponse::Bulk(Bytes::copy_from_slice(value.as_bytes()))
    }
    
    /// Create a simple string response
    #[inline]
    pub fn simple_string(value: &str) -> QuspResponse {
        QuspResponse::Simple(Bytes::copy_from_slice(value.as_bytes()))
    }
    
    /// Create an array response from any iterable of encodable items
    #[inline]
    pub fn array<T, I>(items: I) -> QuspResponse 
    where
        T: Into<QuspResponse>,
        I: IntoIterator<Item = T>,
    {
        QuspResponse::Array(items.into_iter().map(|item| item.into()).collect())
    }
    
    /// Create a response from an optional value, using null for None
    #[inline]
    pub fn optional<T>(value: Option<T>) -> QuspResponse
    where
        T: Into<QuspResponse>,
    {
        match value {
            Some(v) => v.into(),
            None => QuspResponse::Null,
        }
    }
    
    /// Create a paginated response with consistent structure
    #[inline]
    pub fn paginated<T>(result: &PageResult<T>) -> QuspResponse
    where 
        T: Clone + Into<QuspResponse>,
    {
        let mut response = Vec::with_capacity(3); // Pre-allocate for known size
        response.push(ResponseBuilder::array(result.items.iter().cloned().map(|item| item.into())));
        response.push(ResponseBuilder::integer(result.total as i64));
        
        if let Some(cursor) = result.next_cursor {
            response.push(ResponseBuilder::integer(cursor as i64));
        } else {
            response.push(QuspResponse::Null);
        }
        
        QuspResponse::Array(response)
    }
}

/// Trait to convert common types to QuspResponse for use with ResponseBuilder::array
impl From<EntityId> for QuspResponse {
    fn from(id: EntityId) -> Self {
        ResponseBuilder::integer(id.0 as i64)
    }
}

impl From<EntityType> for QuspResponse {
    fn from(entity_type: EntityType) -> Self {
        ResponseBuilder::integer(entity_type.0 as i64)
    }
}

impl From<FieldType> for QuspResponse {
    fn from(field_type: FieldType) -> Self {
        ResponseBuilder::integer(field_type.0 as i64)
    }
}

impl From<bool> for QuspResponse {
    fn from(value: bool) -> Self {
        ResponseBuilder::integer(if value { 1 } else { 0 })
    }
}

/// Message buffer for handling QUSP frames with improved parsing
#[derive(Debug)]
pub struct MessageBuffer {
    buffer: BytesMut,
    max_capacity: usize,
}

impl MessageBuffer {
    /// Create a new message buffer with default capacity
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(64 * 1024)
    }

    /// Create a new message buffer with specified capacity
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
            max_capacity: capacity,
        }
    }

    /// Add data to the buffer
    pub fn add_data(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
        // Prevent buffer from growing too large
        if self.buffer.capacity() > self.max_capacity * 4 {
            let mut new_buffer = BytesMut::with_capacity(self.max_capacity);
            new_buffer.extend_from_slice(&self.buffer);
            self.buffer = new_buffer;
        }
    }

    /// Try to decode a complete frame from the buffer
    pub fn try_decode(&mut self) -> Result<Option<QuspFrame>> {
        match try_parse_message_length(self.buffer.as_ref())? {
            Some(len) => {
                if self.buffer.len() < len {
                    return Ok(None);
                }
                
                let message_bytes = self.buffer.split_to(len);
                let frame = parse_root_frame(&message_bytes.freeze())?;
                
                match frame {
                    RespFrame::Array(_) => {
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
    


    /// Get raw access to the buffer for manual parsing
    #[inline]
    pub fn peek_raw_buffer(&self) -> &[u8] {
        &self.buffer
    }
    
    /// Get the current buffer length
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    
    /// Check if the buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
    
    /// Clear the buffer
    #[inline]
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for MessageBuffer {
	fn default() -> Self {
		Self::new()
	}
}
