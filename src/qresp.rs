use bytes::{Buf, BytesMut};
use thiserror::Error;


#[derive(Debug, Error)]
pub enum QrespError {
    #[error("incomplete frame")]
    Incomplete,
    #[error("invalid frame: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, QrespError>;

#[derive(Debug, Clone, PartialEq)]
pub enum QrespFrame {
    Array(Vec<QrespFrame>),
    Map(Vec<(QrespFrame, QrespFrame)>),
    Bulk(Vec<u8>),
    Integer(i64),
    Boolean(bool),
    Null,
    Error { code: String, message: String },
    Simple(String),
}

pub mod peer {
    use super::{Result as QrespResult, QrespError, QrespFrame};
    use crate::Snapshot;
    use crate::data::Requests;
    use std::convert::TryFrom;

    #[derive(Debug, Clone)]
    pub enum PeerMessage {
        Handshake { start_time: u64, is_response: bool, machine_id: String },
        FullSyncRequest,
        FullSyncResponse { snapshot: Snapshot },
        SyncWrite { requests: Requests },
    }

    pub fn encode_peer_message(message: &PeerMessage) -> QrespResult<QrespFrame> {
        match message {
            PeerMessage::Handshake { start_time, is_response, machine_id } => {
                let start = i64::try_from(*start_time)
                    .map_err(|_| QrespError::Invalid("handshake start_time exceeds i64".to_string()))?;
                Ok(QrespFrame::Array(vec![
                    string_frame("PEER"),
                    string_frame("HANDSHAKE"),
                    QrespFrame::Integer(start),
                    QrespFrame::Boolean(*is_response),
                    string_frame(machine_id),
                ]))
            }
            PeerMessage::FullSyncRequest => Ok(QrespFrame::Array(vec![
                string_frame("PEER"),
                string_frame("FULL_SYNC_REQUEST"),
            ])),
            PeerMessage::FullSyncResponse { snapshot } => {
                let data = serde_json::to_vec(snapshot)
                    .map_err(|e| QrespError::Invalid(format!("snapshot serialization failed: {}", e)))?;
                Ok(QrespFrame::Array(vec![
                    string_frame("PEER"),
                    string_frame("FULL_SYNC_RESPONSE"),
                    QrespFrame::Bulk(data),
                ]))
            }
            PeerMessage::SyncWrite { requests } => {
                let payload = super::store::encode_requests(requests)?;
                Ok(QrespFrame::Array(vec![
                    string_frame("PEER"),
                    string_frame("SYNC_WRITE"),
                    payload,
                ]))
            }
        }
    }

    pub fn decode_peer_message(frame: QrespFrame) -> QrespResult<PeerMessage> {
        match frame {
            QrespFrame::Array(mut items) if !items.is_empty() => {
                ensure_prefix(&items, "PEER")?;
                if items.len() < 2 {
                    return Err(QrespError::Invalid("peer frame missing command".to_string()));
                }
                let command = take_string(&items[1])?;
                match command.as_str() {
                    "HANDSHAKE" => {
                        if items.len() < 5 {
                            return Err(QrespError::Invalid("handshake frame invalid".to_string()));
                        }
                        let start = match &items[2] {
                            QrespFrame::Integer(value) if *value >= 0 => *value as u64,
                            other => {
                                return Err(QrespError::Invalid(format!(
                                    "handshake start_time must be integer, got {:?}",
                                    other
                                )))
                            }
                        };
                        let is_response = match &items[3] {
                            QrespFrame::Boolean(flag) => *flag,
                            other => {
                                return Err(QrespError::Invalid(format!(
                                    "handshake is_response must be boolean, got {:?}",
                                    other
                                )))
                            }
                        };
                        let machine_id = take_string(&items[4])?;
                        Ok(PeerMessage::Handshake { start_time: start, is_response, machine_id })
                    }
                    "FULL_SYNC_REQUEST" => Ok(PeerMessage::FullSyncRequest),
                    "FULL_SYNC_RESPONSE" => {
                        if items.len() < 3 {
                            return Err(QrespError::Invalid("full sync response missing payload".to_string()));
                        }
                        match items.pop() {
                            Some(QrespFrame::Bulk(bytes)) => {
                                let snapshot = serde_json::from_slice(&bytes)
                                    .map_err(|e| QrespError::Invalid(format!("snapshot parse failed: {}", e)))?;
                                Ok(PeerMessage::FullSyncResponse { snapshot })
                            }
                            Some(other) => Err(QrespError::Invalid(format!(
                                "full sync response payload must be bulk, got {:?}",
                                other
                            ))),
                            None => Err(QrespError::Invalid("full sync response missing payload".to_string())),
                        }
                    }
                    "SYNC_WRITE" => {
                        if items.len() < 3 {
                            return Err(QrespError::Invalid("sync write missing requests".to_string()));
                        }
                        let payload = items.pop().unwrap();
                        let requests = super::store::decode_requests(payload)?;
                        Ok(PeerMessage::SyncWrite { requests })
                    }
                    other => Err(QrespError::Invalid(format!("unknown peer command: {}", other))),
                }
            }
            other => Err(QrespError::Invalid(format!(
                "peer message must be array, got {:?}",
                other
            ))),
        }
    }

    fn ensure_prefix(items: &[QrespFrame], expected: &str) -> QrespResult<()> {
        match items.first() {
            Some(frame) if matches_string(frame, expected) => Ok(()),
            Some(other) => Err(QrespError::Invalid(format!(
                "peer message must start with '{}', got {:?}",
                expected,
                other
            ))),
            None => Err(QrespError::Invalid("empty peer frame".to_string())),
        }
    }

    fn matches_string(frame: &QrespFrame, expected: &str) -> bool {
        match frame {
            QrespFrame::Bulk(bytes) => bytes == expected.as_bytes(),
            QrespFrame::Simple(text) => text == expected,
            _ => false,
        }
    }

    fn string_frame(text: &str) -> QrespFrame {
        QrespFrame::Bulk(text.as_bytes().to_vec())
    }

    fn take_string(frame: &QrespFrame) -> QrespResult<String> {
        match frame {
            QrespFrame::Bulk(bytes) => String::from_utf8(bytes.clone())
                .map_err(|e| QrespError::Invalid(format!("invalid UTF-8: {}", e))),
            QrespFrame::Simple(text) => Ok(text.clone()),
            other => Err(QrespError::Invalid(format!(
                "expected string, got {:?}",
                other
            ))),
        }
    }
}

pub use peer::PeerMessage;

#[derive(Debug, Clone)]
pub enum QrespMessage {
    Store(crate::data::StoreMessage),
    Peer(PeerMessage),
}

pub fn encode_message(message: &QrespMessage) -> Result<Vec<u8>> {
    let frame = match message {
        QrespMessage::Store(store) => store::encode_store_message(store)?,
        QrespMessage::Peer(peer) => peer::encode_peer_message(peer)?,
    };
    Ok(QrespCodec::encode(&frame))
}

pub fn decode_message(frame: QrespFrame) -> Result<QrespMessage> {
    if is_peer_frame(&frame) {
        peer::decode_peer_message(frame).map(QrespMessage::Peer)
    } else {
        store::decode_store_message(frame).map(QrespMessage::Store)
    }
}

fn is_peer_frame(frame: &QrespFrame) -> bool {
    match frame {
        QrespFrame::Array(items) => items
            .first()
            .map(|first| matches_string(first, "PEER"))
            .unwrap_or(false),
        _ => false,
    }
}

fn matches_string(frame: &QrespFrame, expected: &str) -> bool {
    match frame {
        QrespFrame::Bulk(bytes) => bytes == expected.as_bytes(),
        QrespFrame::Simple(text) => text == expected,
        _ => false,
    }
}

pub struct QrespCodec;

impl QrespCodec {
    pub fn decode(buffer: &mut BytesMut) -> Result<Option<QrespFrame>> {
        let data = buffer.as_ref();
        if data.is_empty() {
            return Ok(None);
        }
        let mut parser = Parser::new(data);
        match parser.parse_frame()? {
            Some(frame) => {
                let consumed = parser.position();
                buffer.advance(consumed);
                Ok(Some(frame))
            }
            None => Ok(None),
        }
    }

    pub fn encode(frame: &QrespFrame) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(128);
        encode_frame(frame, &mut buffer);
        buffer
    }
}

/// Accumulates incoming TCP bytes and decodes QRESP frames/messages.
#[derive(Debug)]
pub struct QrespMessageBuffer {
    buffer: BytesMut,
    max_capacity: usize,
}

impl QrespMessageBuffer {
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

    pub fn try_decode_frame(&mut self) -> Result<Option<QrespFrame>> {
        QrespCodec::decode(&mut self.buffer)
    }

    pub fn try_decode_message(&mut self) -> Result<Option<QrespMessage>> {
        match self.try_decode_frame()? {
            Some(frame) => decode_message(frame).map(Some),
            None => Ok(None),
        }
    }

    pub fn try_decode_store_message(&mut self) -> Result<Option<crate::data::StoreMessage>> {
        match self.try_decode_frame()? {
            Some(frame) => store::decode_store_message(frame).map(Some),
            None => Ok(None),
        }
    }
}

struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn position(&self) -> usize {
        self.pos
    }

    fn parse_frame(&mut self) -> Result<Option<QrespFrame>> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }
        let prefix = self.data[self.pos];
        match prefix {
            b'*' => self.parse_array(),
            b'~' => self.parse_map(),
            b'$' => self.parse_bulk(),
            b':' => self.parse_integer(),
            b'#' => self.parse_boolean(),
            b'_' => self.parse_null(),
            b'!' => self.parse_error(),
            b'+' => self.parse_simple(),
            _ => Err(QrespError::Invalid(format!("unknown prefix: {}", prefix as char))),
        }
    }

    fn parse_array(&mut self) -> Result<Option<QrespFrame>> {
        let start = self.pos;
        self.pos += 1;
        let len = match self.read_decimal_line()? {
            Some(value) => value,
            None => {
                self.pos = start;
                return Ok(None);
            }
        };
        if len < 0 {
            return Ok(Some(QrespFrame::Null));
        }
        let len = len as usize;
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            match self.parse_frame()? {
                Some(frame) => items.push(frame),
                None => {
                    self.pos = start;
                    return Ok(None);
                }
            }
        }
        Ok(Some(QrespFrame::Array(items)))
    }

    fn parse_map(&mut self) -> Result<Option<QrespFrame>> {
        let start = self.pos;
        self.pos += 1;
        let len = match self.read_decimal_line()? {
            Some(value) => value,
            None => {
                self.pos = start;
                return Ok(None);
            }
        };
        if len < 0 {
            return Ok(Some(QrespFrame::Null));
        }
        let len = len as usize;
        let mut pairs = Vec::with_capacity(len);
        for _ in 0..len {
            let key = match self.parse_frame()? {
                Some(frame) => frame,
                None => {
                    self.pos = start;
                    return Ok(None);
                }
            };
            let value = match self.parse_frame()? {
                Some(frame) => frame,
                None => {
                    self.pos = start;
                    return Ok(None);
                }
            };
            pairs.push((key, value));
        }
        Ok(Some(QrespFrame::Map(pairs)))
    }

    fn parse_bulk(&mut self) -> Result<Option<QrespFrame>> {
        let start = self.pos;
        self.pos += 1;
        let len = match self.read_decimal_line()? {
            Some(value) => value,
            None => {
                self.pos = start;
                return Ok(None);
            }
        };
        if len < 0 {
            return Ok(Some(QrespFrame::Null));
        }
        let len = len as usize;
        let end = self.pos + len + 2;
        if end > self.data.len() {
            self.pos = start;
            return Ok(None);
        }
        let slice = &self.data[self.pos..self.pos + len];
        self.pos += len;
        if !self.consume_crlf() {
            return Err(QrespError::Invalid("bulk string missing CRLF".to_string()));
        }
        Ok(Some(QrespFrame::Bulk(slice.to_vec())))
    }

    fn parse_integer(&mut self) -> Result<Option<QrespFrame>> {
        let start = self.pos;
        self.pos += 1;
        let value = match self.read_decimal_line()? {
            Some(value) => value,
            None => {
                self.pos = start;
                return Ok(None);
            }
        };
        Ok(Some(QrespFrame::Integer(value)))
    }

    fn parse_boolean(&mut self) -> Result<Option<QrespFrame>> {
        let start = self.pos;
        self.pos += 1;
        let line = match self.read_line()? {
            Some(line) => line,
            None => {
                self.pos = start;
                return Ok(None);
            }
        };
        if line.len() != 1 {
            return Err(QrespError::Invalid("boolean must be single byte".to_string()));
        }
        match line[0] {
            b'0' => Ok(Some(QrespFrame::Boolean(false))),
            b'1' => Ok(Some(QrespFrame::Boolean(true))),
            other => Err(QrespError::Invalid(format!("invalid boolean byte: {}", other))),
        }
    }

    fn parse_null(&mut self) -> Result<Option<QrespFrame>> {
        let start = self.pos;
        self.pos += 1;
        match self.read_line()? {
            Some(line) if line.is_empty() => Ok(Some(QrespFrame::Null)),
            Some(_) => Err(QrespError::Invalid("null must be '_' followed by CRLF".to_string())),
            None => {
                self.pos = start;
                Ok(None)
            }
        }
    }

    fn parse_error(&mut self) -> Result<Option<QrespFrame>> {
        let start = self.pos;
        self.pos += 1;
        let line = match self.read_line()? {
            Some(line) => line,
            None => {
                self.pos = start;
                return Ok(None);
            }
        };
        let parts: Vec<&[u8]> = line.splitn(2, |b| *b == b' ').collect();
        if parts.is_empty() {
            return Err(QrespError::Invalid("error must contain code".to_string()));
        }
        let code = String::from_utf8(parts[0].to_vec())
            .map_err(|_| QrespError::Invalid("error code not UTF-8".to_string()))?;
        let message = if parts.len() == 2 {
            String::from_utf8(parts[1].to_vec())
                .map_err(|_| QrespError::Invalid("error message not UTF-8".to_string()))?
        } else {
            String::new()
        };
        Ok(Some(QrespFrame::Error { code, message }))
    }

    fn parse_simple(&mut self) -> Result<Option<QrespFrame>> {
        let start = self.pos;
        self.pos += 1;
        let line = match self.read_line()? {
            Some(line) => line,
            None => {
                self.pos = start;
                return Ok(None);
            }
        };
        let string = String::from_utf8(line.to_vec())
            .map_err(|_| QrespError::Invalid("simple string not UTF-8".to_string()))?;
        Ok(Some(QrespFrame::Simple(string)))
    }

    fn read_decimal_line(&mut self) -> Result<Option<i64>> {
        let line = match self.read_line()? {
            Some(line) => line,
            None => return Ok(None),
        };
        let text = std::str::from_utf8(line)
            .map_err(|_| QrespError::Invalid("invalid decimal".to_string()))?;
        text.parse::<i64>()
            .map(Some)
            .map_err(|_| QrespError::Invalid("invalid decimal".to_string()))
    }

    fn read_line(&mut self) -> Result<Option<&'a [u8]>> {
        let start = self.pos;
        let len = self.data.len();
        let mut idx = start;
        while idx + 1 < len {
            if self.data[idx] == b'\r' && self.data[idx + 1] == b'\n' {
                let line = &self.data[start..idx];
                self.pos = idx + 2;
                return Ok(Some(line));
            }
            idx += 1;
        }
        Ok(None)
    }

    fn consume_crlf(&mut self) -> bool {
        if self.pos + 1 >= self.data.len() {
            return false;
        }
        if self.data[self.pos] == b'\r' && self.data[self.pos + 1] == b'\n' {
            self.pos += 2;
            true
        } else {
            false
        }
    }
}

fn encode_frame(frame: &QrespFrame, buffer: &mut Vec<u8>) {
    match frame {
        QrespFrame::Array(items) => {
            buffer.push(b'*');
            write_decimal(items.len() as i64, buffer);
            buffer.extend_from_slice(b"\r\n");
            for item in items {
                encode_frame(item, buffer);
            }
        }
        QrespFrame::Map(pairs) => {
            buffer.push(b'~');
            write_decimal(pairs.len() as i64, buffer);
            buffer.extend_from_slice(b"\r\n");
            for (k, v) in pairs {
                encode_frame(k, buffer);
                encode_frame(v, buffer);
            }
        }
        QrespFrame::Bulk(bytes) => {
            buffer.push(b'$');
            write_decimal(bytes.len() as i64, buffer);
            buffer.extend_from_slice(b"\r\n");
            buffer.extend_from_slice(bytes);
            buffer.extend_from_slice(b"\r\n");
        }
        QrespFrame::Integer(value) => {
            buffer.push(b':');
            write_decimal(*value, buffer);
            buffer.extend_from_slice(b"\r\n");
        }
        QrespFrame::Boolean(value) => {
            buffer.push(b'#');
            if *value {
                buffer.extend_from_slice(b"1\r\n");
            } else {
                buffer.extend_from_slice(b"0\r\n");
            }
        }
        QrespFrame::Null => {
            buffer.extend_from_slice(b"_\r\n");
        }
        QrespFrame::Error { code, message } => {
            buffer.push(b'!');
            buffer.extend_from_slice(code.as_bytes());
            if !message.is_empty() {
                buffer.push(b' ');
                buffer.extend_from_slice(message.as_bytes());
            }
            buffer.extend_from_slice(b"\r\n");
        }
        QrespFrame::Simple(text) => {
            buffer.push(b'+');
            buffer.extend_from_slice(text.as_bytes());
            buffer.extend_from_slice(b"\r\n");
        }
    }
}

fn write_decimal(value: i64, buffer: &mut Vec<u8>) {
    let text = value.to_string();
    buffer.extend_from_slice(text.as_bytes());
}

pub mod store {
    use super::{Result as QrespResult, QrespError, QrespFrame};
    use crate::data::{
        AdjustBehavior, AuthenticationResult, EntityId, EntitySchema, EntityType, FieldSchema,
        FieldType, Notification, NotifyConfig, PageOpts, PageResult, PushCondition, Request,
        Requests, Value,
    };
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use time::OffsetDateTime;

    pub fn encode_store_message(message: &crate::data::StoreMessage) -> QrespResult<QrespFrame> {
        match message {
            crate::data::StoreMessage::Authenticate { id, subject_name, credential } => {
                Ok(QrespFrame::Array(vec![
                    bulk_str("AUTHENTICATE"),
                    encode_u64_as_integer(*id)?,
                    bulk_str(subject_name),
                    bulk_str(credential),
                ]))
            }
            crate::data::StoreMessage::AuthenticateResponse { id, response } => {
                let payload = match response {
                    Ok(auth) => QrespFrame::Array(vec![
                        bulk_str("OK"),
                        encode_auth_result(auth),
                    ]),
                    Err(err) => QrespFrame::Array(vec![bulk_str("ERR"), bulk_str(err)]),
                };
                Ok(QrespFrame::Array(vec![
                    bulk_str("AUTHENTICATE_RESPONSE"),
                    encode_u64_as_integer(*id)?,
                    payload,
                ]))
            }
            crate::data::StoreMessage::Perform { id, requests } => {
                Ok(QrespFrame::Array(vec![
                    bulk_str("PERFORM"),
                    encode_u64_as_integer(*id)?,
                    encode_requests(requests)?,
                ]))
            }
            crate::data::StoreMessage::PerformResponse { id, response } => {
                let payload = match response {
                    Ok(requests) => QrespFrame::Array(vec![
                        bulk_str("OK"),
                        encode_requests(requests)?,
                    ]),
                    Err(err) => QrespFrame::Array(vec![bulk_str("ERR"), bulk_str(err)]),
                };
                Ok(QrespFrame::Array(vec![
                    bulk_str("PERFORM_RESPONSE"),
                    encode_u64_as_integer(*id)?,
                    payload,
                ]))
            }
            crate::data::StoreMessage::RegisterNotification { id, config } => Ok(QrespFrame::Array(vec![
                bulk_str("REGISTER_NOTIFICATION"),
                encode_u64_as_integer(*id)?,
                encode_notify_config(config)?,
            ])),
            crate::data::StoreMessage::RegisterNotificationResponse { id, response } => {
                let payload = match response {
                    Ok(()) => QrespFrame::Array(vec![bulk_str("OK")]),
                    Err(err) => QrespFrame::Array(vec![bulk_str("ERR"), bulk_str(err)]),
                };
                Ok(QrespFrame::Array(vec![
                    bulk_str("REGISTER_NOTIFICATION_RESPONSE"),
                    encode_u64_as_integer(*id)?,
                    payload,
                ]))
            }
            crate::data::StoreMessage::UnregisterNotification { id, config } => Ok(QrespFrame::Array(vec![
                bulk_str("UNREGISTER_NOTIFICATION"),
                encode_u64_as_integer(*id)?,
                encode_notify_config(config)?,
            ])),
            crate::data::StoreMessage::UnregisterNotificationResponse { id, response } => Ok(QrespFrame::Array(vec![
                bulk_str("UNREGISTER_NOTIFICATION_RESPONSE"),
                encode_u64_as_integer(*id)?,
                QrespFrame::Boolean(*response),
            ])),
            crate::data::StoreMessage::Notification { notification } => Ok(QrespFrame::Array(vec![
                bulk_str("NOTIFICATION"),
                encode_notification(notification)?,
            ])),
            crate::data::StoreMessage::Error { id, error } => Ok(QrespFrame::Array(vec![
                bulk_str("ERROR"),
                encode_u64_as_integer(*id)?,
                bulk_str(error),
            ])),
        }
    }

    pub fn decode_store_message(frame: QrespFrame) -> QrespResult<crate::data::StoreMessage> {
        match frame {
            QrespFrame::Array(mut items) => {
                if items.is_empty() {
                    return Err(QrespError::Invalid("empty frame".to_string()));
                }
                let command = take_string(&mut items.remove(0))?;
                match command.as_str() {
                    "AUTHENTICATE" => {
                        ensure_len(&items, 3, "AUTHENTICATE")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let subject_name = take_string(&items[1].clone())?;
                        let credential = take_string(&items[2].clone())?;
                        Ok(crate::data::StoreMessage::Authenticate {
                            id,
                            subject_name,
                            credential,
                        })
                    }
                    "AUTHENTICATE_RESPONSE" => {
                        ensure_len(&items, 2, "AUTHENTICATE_RESPONSE")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let response = decode_auth_response(items[1].clone())?;
                        Ok(crate::data::StoreMessage::AuthenticateResponse { id, response })
                    }
                    "PERFORM" => {
                        ensure_len(&items, 2, "PERFORM")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let requests = decode_requests(items[1].clone())?;
                        Ok(crate::data::StoreMessage::Perform { id, requests })
                    }
                    "PERFORM_RESPONSE" => {
                        ensure_len(&items, 2, "PERFORM_RESPONSE")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let response = decode_perform_response(items[1].clone())?;
                        Ok(crate::data::StoreMessage::PerformResponse { id, response })
                    }
                    "REGISTER_NOTIFICATION" => {
                        ensure_len(&items, 2, "REGISTER_NOTIFICATION")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let config = decode_notify_config(items[1].clone())?;
                        Ok(crate::data::StoreMessage::RegisterNotification { id, config })
                    }
                    "REGISTER_NOTIFICATION_RESPONSE" => {
                        ensure_len(&items, 2, "REGISTER_NOTIFICATION_RESPONSE")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let response = decode_register_notification_response(items[1].clone())?;
                        Ok(crate::data::StoreMessage::RegisterNotificationResponse { id, response })
                    }
                    "UNREGISTER_NOTIFICATION" => {
                        ensure_len(&items, 2, "UNREGISTER_NOTIFICATION")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let config = decode_notify_config(items[1].clone())?;
                        Ok(crate::data::StoreMessage::UnregisterNotification { id, config })
                    }
                    "UNREGISTER_NOTIFICATION_RESPONSE" => {
                        ensure_len(&items, 2, "UNREGISTER_NOTIFICATION_RESPONSE")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let response = match &items[1] {
                            QrespFrame::Boolean(flag) => *flag,
                            other => {
                                return Err(QrespError::Invalid(format!(
                                    "expected boolean response, got {:?}",
                                    other
                                )))
                            }
                        };
                        Ok(crate::data::StoreMessage::UnregisterNotificationResponse { id, response })
                    }
                    "NOTIFICATION" => {
                        ensure_len(&items, 1, "NOTIFICATION")?;
                        let notification = decode_notification(items[0].clone())?;
                        Ok(crate::data::StoreMessage::Notification { notification })
                    }
                    "ERROR" => {
                        ensure_len(&items, 2, "ERROR")?;
                        let id = decode_u64_from_integer(&items[0])?;
                        let error = take_string(&items[1])?;
                        Ok(crate::data::StoreMessage::Error { id, error })
                    }
                    other => Err(QrespError::Invalid(format!("unknown command: {}", other))),
                }
            }
            _ => Err(QrespError::Invalid("store message must be array".to_string())),
        }
    }

    pub(crate) fn encode_requests(requests: &Requests) -> QrespResult<QrespFrame> {
        let origin = requests.originator().map(encode_entity_id).unwrap_or_else(|| QrespFrame::Null);
        let guard = requests.read();
        let mut frames = Vec::with_capacity(guard.len());
        for request in guard.iter() {
            frames.push(encode_request(request)?);
        }
        Ok(QrespFrame::Array(vec![origin, QrespFrame::Array(frames)]))
    }

    pub(crate) fn decode_requests(frame: QrespFrame) -> QrespResult<Requests> {
        match frame {
            QrespFrame::Array(mut parts) if parts.len() == 2 => {
                let origin = decode_optional_entity_id(Some(parts.remove(0)))?;
                let requests_array = parts.remove(0);
                let list = match requests_array {
                    QrespFrame::Array(items) => {
                        let mut result = Vec::with_capacity(items.len());
                        for item in items {
                            result.push(decode_request(item)?);
                        }
                        result
                    }
                    other => {
                        return Err(QrespError::Invalid(format!(
                            "expected array of requests, got {:?}",
                            other
                        )))
                    }
                };
                let requests = Requests::new(list);
                requests.set_originator(origin);
                Ok(requests)
            }
            other => Err(QrespError::Invalid(format!(
                "requests must be [origin, array], got {:?}",
                other
            ))),
        }
    }

    fn encode_request(request: &Request) -> QrespResult<QrespFrame> {
        match request {
            Request::Read { entity_id, field_types, value, write_time, writer_id } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("read")),
                (bulk_str("entity_id"), encode_entity_id(*entity_id)),
                (bulk_str("fields"), encode_field_types(field_types)),
                (bulk_str("value"), encode_option_value(value)?),
                (bulk_str("write_time"), encode_option_timestamp(*write_time)),
                (bulk_str("writer_id"), encode_option_entity_id(*writer_id)),
            ])),
            Request::Write { entity_id, field_types, value, push_condition, adjust_behavior, write_time, writer_id, write_processed } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("write")),
                (bulk_str("entity_id"), encode_entity_id(*entity_id)),
                (bulk_str("fields"), encode_field_types(field_types)),
                (bulk_str("value"), encode_option_value(value)?),
                (bulk_str("push_condition"), bulk_str(match push_condition {
                    PushCondition::Always => "always",
                    PushCondition::Changes => "changes",
                })),
                (bulk_str("adjust_behavior"), bulk_str(match adjust_behavior {
                    AdjustBehavior::Set => "set",
                    AdjustBehavior::Add => "add",
                    AdjustBehavior::Subtract => "subtract",
                })),
                (bulk_str("write_time"), encode_option_timestamp(*write_time)),
                (bulk_str("writer_id"), encode_option_entity_id(*writer_id)),
                (bulk_str("write_processed"), QrespFrame::Boolean(*write_processed)),
            ])),
            Request::Create { entity_type, parent_id, name, created_entity_id, timestamp } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("create")),
                (bulk_str("entity_type"), encode_entity_type(*entity_type)),
                (bulk_str("parent_id"), encode_option_entity_id(*parent_id)),
                (bulk_str("name"), bulk_str(name)),
                (bulk_str("created_entity_id"), encode_option_entity_id(*created_entity_id)),
                (bulk_str("timestamp"), encode_option_timestamp(*timestamp)),
            ])),
            Request::Delete { entity_id, timestamp } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("delete")),
                (bulk_str("entity_id"), encode_entity_id(*entity_id)),
                (bulk_str("timestamp"), encode_option_timestamp(*timestamp)),
            ])),
            Request::SchemaUpdate { schema, timestamp } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("schema_update")),
                (bulk_str("schema"), encode_schema(schema)?),
                (bulk_str("timestamp"), encode_option_timestamp(*timestamp)),
            ])),
            Request::Snapshot { snapshot_counter, timestamp } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("snapshot")),
                (bulk_str("counter"), QrespFrame::Integer(*snapshot_counter as i64)),
                (bulk_str("timestamp"), encode_option_timestamp(*timestamp)),
            ])),
            Request::GetEntityType { name, entity_type } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("get_entity_type")),
                (bulk_str("name"), bulk_str(name)),
                (bulk_str("entity_type"), encode_option_entity_type(*entity_type)),
            ])),
            Request::ResolveEntityType { entity_type, name } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("resolve_entity_type")),
                (bulk_str("entity_type"), encode_entity_type(*entity_type)),
                (bulk_str("name"), encode_option_string(name)),
            ])),
            Request::GetFieldType { name, field_type } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("get_field_type")),
                (bulk_str("name"), bulk_str(name)),
                (bulk_str("field_type"), encode_option_field_type(*field_type)),
            ])),
            Request::ResolveFieldType { field_type, name } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("resolve_field_type")),
                (bulk_str("field_type"), encode_field_type(*field_type)),
                (bulk_str("name"), encode_option_string(name)),
            ])),
            Request::GetEntitySchema { entity_type, schema } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("get_entity_schema")),
                (bulk_str("entity_type"), encode_entity_type(*entity_type)),
                (bulk_str("schema"), encode_option_entity_schema_single(schema)?),
            ])),
            Request::GetCompleteEntitySchema { entity_type, schema } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("get_complete_entity_schema")),
                (bulk_str("entity_type"), encode_entity_type(*entity_type)),
                (bulk_str("schema"), encode_option_entity_schema_complete(schema)?),
            ])),
            Request::GetFieldSchema { entity_type, field_type, schema } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("get_field_schema")),
                (bulk_str("entity_type"), encode_entity_type(*entity_type)),
                (bulk_str("field_type"), encode_field_type(*field_type)),
                (bulk_str("schema"), encode_option_field_schema(schema)?),
            ])),
            Request::EntityExists { entity_id, exists } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("entity_exists")),
                (bulk_str("entity_id"), encode_entity_id(*entity_id)),
                (bulk_str("exists"), encode_option_bool(*exists)),
            ])),
            Request::FieldExists { entity_type, field_type, exists } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("field_exists")),
                (bulk_str("entity_type"), encode_entity_type(*entity_type)),
                (bulk_str("field_type"), encode_field_type(*field_type)),
                (bulk_str("exists"), encode_option_bool(*exists)),
            ])),
            Request::FindEntities { entity_type, page_opts, filter, result } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("find_entities")),
                (bulk_str("entity_type"), encode_entity_type(*entity_type)),
                (bulk_str("page_opts"), encode_option_page_opts(page_opts)?),
                (bulk_str("filter"), encode_option_string(filter)),
                (bulk_str("result"), encode_option_page_result(result)?),
            ])),
            Request::FindEntitiesExact { entity_type, page_opts, filter, result } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("find_entities_exact")),
                (bulk_str("entity_type"), encode_entity_type(*entity_type)),
                (bulk_str("page_opts"), encode_option_page_opts(page_opts)?),
                (bulk_str("filter"), encode_option_string(filter)),
                (bulk_str("result"), encode_option_page_result(result)?),
            ])),
            Request::GetEntityTypes { page_opts, result } => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("get_entity_types")),
                (bulk_str("page_opts"), encode_option_page_opts(page_opts)?),
                (bulk_str("result"), encode_option_page_entity_type(result)?),
            ])),
        }
    }

    fn decode_request(frame: QrespFrame) -> QrespResult<Request> {
        let map = match frame {
            QrespFrame::Map(entries) => map_from_entries(entries)?,
            other => {
                return Err(QrespError::Invalid(format!(
                    "request frame must be map, got {:?}",
                    other
                )))
            }
        };
        let rtype = require_string(&map, "type")?;
        match rtype.as_str() {
            "read" => Ok(Request::Read {
                entity_id: decode_entity_id(require_frame(&map, "entity_id")?)?,
                field_types: decode_field_types(require_frame(&map, "fields")?)?,
                value: decode_option_value(optional_frame_owned(&map, "value")?)?,
                write_time: decode_option_timestamp(optional_frame_owned(&map, "write_time")?)?,
                writer_id: decode_optional_entity_id(optional_frame_owned(&map, "writer_id")?)?,
            }),
            "write" => Ok(Request::Write {
                entity_id: decode_entity_id(require_frame(&map, "entity_id")?)?,
                field_types: decode_field_types(require_frame(&map, "fields")?)?,
                value: decode_option_value(optional_frame_owned(&map, "value")?)?,
                push_condition: decode_push_condition(require_string(&map, "push_condition")?),
                adjust_behavior: decode_adjust_behavior(require_string(&map, "adjust_behavior")?),
                write_time: decode_option_timestamp(optional_frame_owned(&map, "write_time")?)?,
                writer_id: decode_optional_entity_id(optional_frame_owned(&map, "writer_id")?)?,
                write_processed: match optional_frame(&map, "write_processed")? {
                    Some(QrespFrame::Boolean(flag)) => *flag,
                    Some(other) => {
                        return Err(QrespError::Invalid(format!(
                            "write_processed must be boolean, got {:?}",
                            other
                        )))
                    }
                    None => false,
                },
            }),
            "create" => Ok(Request::Create {
                entity_type: decode_entity_type(require_frame(&map, "entity_type")?)?,
                parent_id: decode_optional_entity_id(optional_frame_owned(&map, "parent_id")?)?,
                name: require_string(&map, "name")?,
                created_entity_id: decode_optional_entity_id(optional_frame_owned(&map, "created_entity_id")?)?,
                timestamp: decode_option_timestamp(optional_frame_owned(&map, "timestamp")?)?,
            }),
            "delete" => Ok(Request::Delete {
                entity_id: decode_entity_id(require_frame(&map, "entity_id")?)?,
                timestamp: decode_option_timestamp(optional_frame_owned(&map, "timestamp")?)?,
            }),
            "schema_update" => Ok(Request::SchemaUpdate {
                schema: decode_entity_schema(require_frame(&map, "schema")?)?,
                timestamp: decode_option_timestamp(optional_frame_owned(&map, "timestamp")?)?,
            }),
            "snapshot" => Ok(Request::Snapshot {
                snapshot_counter: match require_frame(&map, "counter")? {
                    QrespFrame::Integer(value) => *value as u64,
                    other => {
                        return Err(QrespError::Invalid(format!(
                            "snapshot counter must be integer, got {:?}",
                            other
                        )))
                    }
                },
                timestamp: decode_option_timestamp(optional_frame_owned(&map, "timestamp")?)?,
            }),
            "get_entity_type" => Ok(Request::GetEntityType {
                name: require_string(&map, "name")?,
                entity_type: decode_optional_entity_type(optional_frame_owned(&map, "entity_type")?)?,
            }),
            "resolve_entity_type" => Ok(Request::ResolveEntityType {
                entity_type: decode_entity_type(require_frame(&map, "entity_type")?)?,
                name: decode_option_string(optional_frame(&map, "name")?)?,
            }),
            "get_field_type" => Ok(Request::GetFieldType {
                name: require_string(&map, "name")?,
                field_type: decode_optional_field_type(optional_frame_owned(&map, "field_type")?)?,
            }),
            "resolve_field_type" => Ok(Request::ResolveFieldType {
                field_type: decode_field_type(require_frame(&map, "field_type")?)?,
                name: decode_option_string(optional_frame(&map, "name")?)?,
            }),
            "get_entity_schema" => Ok(Request::GetEntitySchema {
                entity_type: decode_entity_type(require_frame(&map, "entity_type")?)?,
                schema: decode_option_entity_schema_single(optional_frame(&map, "schema")?)?,
            }),
            "get_complete_entity_schema" => Ok(Request::GetCompleteEntitySchema {
                entity_type: decode_entity_type(require_frame(&map, "entity_type")?)?,
                schema: decode_option_entity_schema_complete(optional_frame(&map, "schema")?)?,
            }),
            "get_field_schema" => Ok(Request::GetFieldSchema {
                entity_type: decode_entity_type(require_frame(&map, "entity_type")?)?,
                field_type: decode_field_type(require_frame(&map, "field_type")?)?,
                schema: decode_option_field_schema(optional_frame(&map, "schema")?)?,
            }),
            "entity_exists" => Ok(Request::EntityExists {
                entity_id: decode_entity_id(require_frame(&map, "entity_id")?)?,
                exists: decode_option_bool(optional_frame(&map, "exists")?)?,
            }),
            "field_exists" => Ok(Request::FieldExists {
                entity_type: decode_entity_type(require_frame(&map, "entity_type")?)?,
                field_type: decode_field_type(require_frame(&map, "field_type")?)?,
                exists: decode_option_bool(optional_frame(&map, "exists")?)?,
            }),
            "find_entities" => Ok(Request::FindEntities {
                entity_type: decode_entity_type(require_frame(&map, "entity_type")?)?,
                page_opts: decode_option_page_opts(optional_frame(&map, "page_opts")?)?,
                filter: decode_option_string(optional_frame(&map, "filter")?)?,
                result: decode_option_page_result(optional_frame(&map, "result")?)?,
            }),
            "find_entities_exact" => Ok(Request::FindEntitiesExact {
                entity_type: decode_entity_type(require_frame(&map, "entity_type")?)?,
                page_opts: decode_option_page_opts(optional_frame(&map, "page_opts")?)?,
                filter: decode_option_string(optional_frame(&map, "filter")?)?,
                result: decode_option_page_result(optional_frame(&map, "result")?)?,
            }),
            "get_entity_types" => Ok(Request::GetEntityTypes {
                page_opts: decode_option_page_opts(optional_frame(&map, "page_opts")?)?,
                result: decode_option_page_entity_type(optional_frame(&map, "result")?)?,
            }),
            other => Err(QrespError::Invalid(format!("unknown request type: {}", other))),
        }
    }

    fn encode_auth_result(result: &AuthenticationResult) -> QrespFrame {
        QrespFrame::Map(vec![
            (bulk_str("subject_id"), encode_entity_id(result.subject_id)),
        ])
    }

    fn decode_auth_response(frame: QrespFrame) -> QrespResult<std::result::Result<AuthenticationResult, String>> {
        match frame {
            QrespFrame::Array(mut items) if !items.is_empty() => {
                let marker = take_string(&items.remove(0))?;
                match marker.as_str() {
                    "OK" => {
                        ensure_len(&items, 1, "AUTHENTICATE_RESPONSE OK")?;
                        let map = match items.remove(0) {
                            QrespFrame::Map(entries) => map_from_entries(entries)?,
                            other => {
                                return Err(QrespError::Invalid(format!(
                                    "auth payload must be map, got {:?}",
                                    other
                                )))
                            }
                        };
                        let subject_id = decode_entity_id(require_frame(&map, "subject_id")?)?;
                        Ok(Ok(AuthenticationResult { subject_id }))
                    }
                    "ERR" => {
                        let message = if items.is_empty() {
                            String::new()
                        } else {
                            take_string(&items[0])?
                        };
                        Ok(Err(message))
                    }
                    other => Err(QrespError::Invalid(format!(
                        "unexpected auth response marker: {}",
                        other
                    ))),
                }
            }
            other => Err(QrespError::Invalid(format!(
                "auth response must be array, got {:?}",
                other
            ))),
        }
    }

    fn decode_perform_response(frame: QrespFrame) -> QrespResult<std::result::Result<Requests, String>> {
        match frame {
            QrespFrame::Array(mut items) if !items.is_empty() => {
                let marker = take_string(&items.remove(0))?;
                match marker.as_str() {
                    "OK" => {
                        ensure_len(&items, 1, "PERFORM_RESPONSE OK")?;
                        let requests = decode_requests(items.remove(0))?;
                        Ok(Ok(requests))
                    }
                    "ERR" => {
                        let message = if items.is_empty() {
                            String::new()
                        } else {
                            take_string(&items[0])?
                        };
                        Ok(Err(message))
                    }
                    other => Err(QrespError::Invalid(format!(
                        "unexpected perform response marker: {}",
                        other
                    ))),
                }
            }
            other => Err(QrespError::Invalid(format!(
                "perform response must be array, got {:?}",
                other
            ))),
        }
    }

    fn encode_notify_config(config: &NotifyConfig) -> QrespResult<QrespFrame> {
        let data = serde_json::to_vec(config)
            .map_err(|e| QrespError::Invalid(format!("notify config serialization failed: {}", e)))?;
        Ok(QrespFrame::Bulk(data))
    }

    fn decode_notify_config(frame: QrespFrame) -> QrespResult<NotifyConfig> {
        match frame {
            QrespFrame::Bulk(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| QrespError::Invalid(format!("notify config parse failed: {}", e))),
            other => Err(QrespError::Invalid(format!(
                "notify config must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_notification(notification: &Notification) -> QrespResult<QrespFrame> {
        let data = serde_json::to_vec(notification)
            .map_err(|e| QrespError::Invalid(format!("notification serialization failed: {}", e)))?;
        Ok(QrespFrame::Bulk(data))
    }

    fn decode_notification(frame: QrespFrame) -> QrespResult<Notification> {
        match frame {
            QrespFrame::Bulk(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| QrespError::Invalid(format!("notification parse failed: {}", e))),
            other => Err(QrespError::Invalid(format!(
                "notification must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn decode_register_notification_response(frame: QrespFrame) -> QrespResult<std::result::Result<(), String>> {
        match frame {
            QrespFrame::Array(mut items) if !items.is_empty() => {
                let marker = take_string(&items.remove(0))?;
                match marker.as_str() {
                    "OK" => Ok(Ok(())),
                    "ERR" => {
                        let message = if items.is_empty() {
                            String::new()
                        } else {
                            take_string(&items[0])?
                        };
                        Ok(Err(message))
                    }
                    other => Err(QrespError::Invalid(format!(
                        "unexpected register response marker: {}",
                        other
                    ))),
                }
            }
            other => Err(QrespError::Invalid(format!(
                "register notification response must be array, got {:?}",
                other
            ))),
        }
    }

    fn encode_entity_id(id: EntityId) -> QrespFrame {
        QrespFrame::Bulk(id.0.to_be_bytes().to_vec())
    }

    fn decode_entity_id(frame: &QrespFrame) -> QrespResult<EntityId> {
        match frame {
            QrespFrame::Bulk(bytes) if bytes.len() == 8 => {
                let mut array = [0u8; 8];
                array.copy_from_slice(bytes);
                Ok(EntityId(u64::from_be_bytes(array)))
            }
            other => Err(QrespError::Invalid(format!(
                "entity id must be 8-byte bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_entity_type(entity_type: EntityType) -> QrespFrame {
        QrespFrame::Bulk(entity_type.0.to_be_bytes().to_vec())
    }

    fn decode_entity_type(frame: &QrespFrame) -> QrespResult<EntityType> {
        match frame {
            QrespFrame::Bulk(bytes) if bytes.len() == 4 => {
                let mut array = [0u8; 4];
                array.copy_from_slice(bytes);
                Ok(EntityType(u32::from_be_bytes(array)))
            }
            other => Err(QrespError::Invalid(format!(
                "entity type must be 4-byte bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_field_type(field_type: FieldType) -> QrespFrame {
        QrespFrame::Bulk(field_type.0.to_be_bytes().to_vec())
    }

    fn decode_field_type(frame: &QrespFrame) -> QrespResult<FieldType> {
        match frame {
            QrespFrame::Bulk(bytes) if bytes.len() == 8 => {
                let mut array = [0u8; 8];
                array.copy_from_slice(bytes);
                Ok(FieldType(u64::from_be_bytes(array)))
            }
            other => Err(QrespError::Invalid(format!(
                "field type must be 8-byte bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_field_types(field_types: &crate::data::IndirectFieldType) -> QrespFrame {
        let items = field_types
            .iter()
            .map(|ft| encode_field_type(*ft))
            .collect::<Vec<_>>();
        QrespFrame::Array(items)
    }

    fn decode_field_types(frame: &QrespFrame) -> QrespResult<crate::data::IndirectFieldType> {
        match frame {
            QrespFrame::Array(items) => {
                let mut field_types = crate::data::IndirectFieldType::new();
                for item in items {
                    field_types.push(decode_field_type(item)?);
                }
                Ok(field_types)
            }
            other => Err(QrespError::Invalid(format!(
                "field types must be array, got {:?}",
                other
            ))),
        }
    }

    fn encode_option_entity_id(value: Option<EntityId>) -> QrespFrame {
        match value {
            Some(id) => encode_entity_id(id),
            None => QrespFrame::Null,
        }
    }

    fn decode_optional_entity_id(frame: Option<QrespFrame>) -> QrespResult<Option<EntityId>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(other) => decode_entity_id(&other).map(Some),
        }
    }

    fn encode_option_entity_type(value: Option<EntityType>) -> QrespFrame {
        match value {
            Some(entity_type) => encode_entity_type(entity_type),
            None => QrespFrame::Null,
        }
    }

    fn decode_optional_entity_type(frame: Option<QrespFrame>) -> QrespResult<Option<EntityType>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(other) => decode_entity_type(&other).map(Some),
        }
    }

    fn encode_option_field_type(value: Option<FieldType>) -> QrespFrame {
        match value {
            Some(field_type) => encode_field_type(field_type),
            None => QrespFrame::Null,
        }
    }

    fn decode_optional_field_type(frame: Option<QrespFrame>) -> QrespResult<Option<FieldType>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(other) => decode_field_type(&other).map(Some),
        }
    }

    fn encode_option_value(value: &Option<Value>) -> QrespResult<QrespFrame> {
        match value {
            Some(v) => Ok(encode_value(v)?),
            None => Ok(QrespFrame::Null),
        }
    }

    fn decode_option_value(frame: Option<QrespFrame>) -> QrespResult<Option<Value>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(other) => Ok(Some(decode_value(other)?)),
        }
    }

    fn encode_value(value: &Value) -> QrespResult<QrespFrame> {
        match value {
            Value::Blob(blob) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("blob")),
                (bulk_str("data"), QrespFrame::Bulk(blob.to_vec())),
            ])),
            Value::Bool(flag) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("bool")),
                (bulk_str("data"), QrespFrame::Boolean(*flag)),
            ])),
            Value::Choice(choice) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("choice")),
                (bulk_str("data"), QrespFrame::Integer(*choice)),
            ])),
            Value::EntityList(list) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("entity_list")),
                (bulk_str("data"), QrespFrame::Array(list.iter().map(|id| encode_entity_id(*id)).collect())),
            ])),
            Value::EntityReference(reference) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("entity_reference")),
                (bulk_str("data"), encode_option_entity_id(*reference)),
            ])),
            Value::Float(value) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("float")),
                (bulk_str("data"), QrespFrame::Bulk(value.to_be_bytes().to_vec())),
            ])),
            Value::Int(value) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("int")),
                (bulk_str("data"), QrespFrame::Integer(*value)),
            ])),
            Value::String(value) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("string")),
                (bulk_str("data"), QrespFrame::Bulk(value.to_string().into_bytes())),
            ])),
            Value::Timestamp(timestamp) => Ok(QrespFrame::Map(vec![
                (bulk_str("type"), bulk_str("timestamp")),
                (bulk_str("data"), encode_timestamp(*timestamp)),
            ])),
        }
    }

    fn decode_value(frame: QrespFrame) -> QrespResult<Value> {
        let map = match frame {
            QrespFrame::Map(entries) => map_from_entries(entries)?,
            other => {
                return Err(QrespError::Invalid(format!(
                    "value must be map, got {:?}",
                    other
                )))
            }
        };
        let vtype = require_string(&map, "type")?;
        let data_frame = require_frame(&map, "data")?.clone();
        match vtype.as_str() {
            "blob" => match data_frame {
                QrespFrame::Bulk(bytes) => Ok(Value::from_blob(bytes)),
                other => Err(QrespError::Invalid(format!(
                    "blob data must be bulk, got {:?}",
                    other
                ))),
            },
            "bool" => match data_frame {
                QrespFrame::Boolean(flag) => Ok(Value::from_bool(flag)),
                other => Err(QrespError::Invalid(format!(
                    "bool data must be boolean, got {:?}",
                    other
                ))),
            },
            "choice" => match data_frame {
                QrespFrame::Integer(value) => Ok(Value::from_choice(value)),
                other => Err(QrespError::Invalid(format!(
                    "choice data must be integer, got {:?}",
                    other
                ))),
            },
            "entity_list" => match data_frame {
                QrespFrame::Array(items) => {
                    let mut list = Vec::with_capacity(items.len());
                    for item in items {
                        list.push(decode_entity_id(&item)?);
                    }
                    Ok(Value::from_entity_list(list))
                }
                other => Err(QrespError::Invalid(format!(
                    "entity_list data must be array, got {:?}",
                    other
                ))),
            },
            "entity_reference" => {
                let reference = decode_optional_entity_id(Some(data_frame))?;
                Ok(Value::from_entity_reference(reference))
            }
            "float" => match data_frame {
                QrespFrame::Bulk(bytes) if bytes.len() == 8 => {
                    let mut array = [0u8; 8];
                    array.copy_from_slice(&bytes);
                    Ok(Value::from_float(f64::from_be_bytes(array)))
                }
                other => Err(QrespError::Invalid(format!(
                    "float data must be 8-byte bulk, got {:?}",
                    other
                ))),
            },
            "int" => match data_frame {
                QrespFrame::Integer(value) => Ok(Value::from_int(value)),
                other => Err(QrespError::Invalid(format!(
                    "int data must be integer, got {:?}",
                    other
                ))),
            },
            "string" => match data_frame {
                QrespFrame::Bulk(bytes) => Ok(Value::from_string(String::from_utf8(bytes)
                    .map_err(|e| QrespError::Invalid(format!("string value invalid UTF-8: {}", e)))?)),
                other => Err(QrespError::Invalid(format!(
                    "string data must be bulk, got {:?}",
                    other
                ))),
            },
            "timestamp" => Ok(Value::from_timestamp(decode_timestamp(data_frame)?)),
            other => Err(QrespError::Invalid(format!("unknown value type: {}", other))),
        }
    }

    fn encode_timestamp(timestamp: OffsetDateTime) -> QrespFrame {
        let seconds = timestamp.unix_timestamp();
        let nanos = timestamp.nanosecond() as i64;
        QrespFrame::Array(vec![
            QrespFrame::Integer(seconds),
            QrespFrame::Integer(nanos),
        ])
    }

    fn decode_timestamp(frame: QrespFrame) -> QrespResult<OffsetDateTime> {
        match frame {
            QrespFrame::Array(items) if items.len() == 2 => {
                let seconds = match &items[0] {
                    QrespFrame::Integer(value) => *value,
                    other => {
                        return Err(QrespError::Invalid(format!(
                            "timestamp seconds must be integer, got {:?}",
                            other
                        )))
                    }
                };
                let nanos = match &items[1] {
                    QrespFrame::Integer(value) => *value,
                    other => {
                        return Err(QrespError::Invalid(format!(
                            "timestamp nanos must be integer, got {:?}",
                            other
                        )))
                    }
                };
                OffsetDateTime::from_unix_timestamp(seconds)
                    .map_err(|e| QrespError::Invalid(format!("invalid timestamp: {}", e)))?
                    .replace_nanosecond(u32::try_from(nanos).unwrap_or(0))
                    .map_err(|e| QrespError::Invalid(format!("invalid nanoseconds: {}", e)))
            }
            other => Err(QrespError::Invalid(format!(
                "timestamp must be [seconds, nanos], got {:?}",
                other
            ))),
        }
    }

    fn encode_option_timestamp(timestamp: Option<OffsetDateTime>) -> QrespFrame {
        match timestamp {
            Some(ts) => encode_timestamp(ts),
            None => QrespFrame::Null,
        }
    }

    fn decode_option_timestamp(frame: Option<QrespFrame>) -> QrespResult<Option<OffsetDateTime>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(other) => decode_timestamp(other).map(Some),
        }
    }

    fn encode_option_string(value: &Option<String>) -> QrespFrame {
        match value {
            Some(text) => bulk_str(text),
            None => QrespFrame::Null,
        }
    }

    fn decode_option_string(frame: Option<&QrespFrame>) -> QrespResult<Option<String>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(other) => take_string(other).map(Some),
        }
    }

    fn encode_option_bool(value: Option<bool>) -> QrespFrame {
        match value {
            Some(flag) => QrespFrame::Boolean(flag),
            None => QrespFrame::Null,
        }
    }

    fn decode_option_bool(frame: Option<&QrespFrame>) -> QrespResult<Option<bool>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(QrespFrame::Boolean(flag)) => Ok(Some(*flag)),
            Some(other) => Err(QrespError::Invalid(format!(
                "option bool must be boolean, got {:?}",
                other
            ))),
        }
    }

    fn encode_schema(schema: &EntitySchema<crate::data::Single, String, String>) -> QrespResult<QrespFrame> {
        let data = serde_json::to_vec(schema)
            .map_err(|e| QrespError::Invalid(format!("schema serialization failed: {}", e)))?;
        Ok(QrespFrame::Bulk(data))
    }

    fn decode_entity_schema(frame: &QrespFrame) -> QrespResult<EntitySchema<crate::data::Single, String, String>> {
        match frame {
            QrespFrame::Bulk(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| QrespError::Invalid(format!("schema parse failed: {}", e))),
            other => Err(QrespError::Invalid(format!(
                "schema must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_option_entity_schema_single(
        schema: &Option<EntitySchema<crate::data::Single>>,
    ) -> QrespResult<QrespFrame> {
        match schema {
            Some(schema) => {
                let data = serde_json::to_vec(schema)
                    .map_err(|e| QrespError::Invalid(format!("entity schema serialization failed: {}", e)))?;
                Ok(QrespFrame::Bulk(data))
            }
            None => Ok(QrespFrame::Null),
        }
    }

    fn encode_option_entity_schema_complete(
        schema: &Option<EntitySchema<crate::data::Complete>>,
    ) -> QrespResult<QrespFrame> {
        match schema {
            Some(schema) => {
                let data = serde_json::to_vec(schema)
                    .map_err(|e| QrespError::Invalid(format!("entity schema serialization failed: {}", e)))?;
                Ok(QrespFrame::Bulk(data))
            }
            None => Ok(QrespFrame::Null),
        }
    }

    fn decode_option_entity_schema_single(
        frame: Option<&QrespFrame>,
    ) -> QrespResult<Option<EntitySchema<crate::data::Single>>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(QrespFrame::Bulk(bytes)) => serde_json::from_slice(bytes)
                .map(Some)
                .map_err(|e| QrespError::Invalid(format!("entity schema parse failed: {}", e))),
            Some(other) => Err(QrespError::Invalid(format!(
                "entity schema must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn decode_option_entity_schema_complete(
        frame: Option<&QrespFrame>,
    ) -> QrespResult<Option<EntitySchema<crate::data::Complete>>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(QrespFrame::Bulk(bytes)) => serde_json::from_slice(bytes)
                .map(Some)
                .map_err(|e| QrespError::Invalid(format!("entity schema parse failed: {}", e))),
            Some(other) => Err(QrespError::Invalid(format!(
                "entity schema must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_option_field_schema(schema: &Option<FieldSchema>) -> QrespResult<QrespFrame> {
        match schema {
            Some(schema) => {
                let data = serde_json::to_vec(schema)
                    .map_err(|e| QrespError::Invalid(format!("field schema serialization failed: {}", e)))?;
                Ok(QrespFrame::Bulk(data))
            }
            None => Ok(QrespFrame::Null),
        }
    }

    fn decode_option_field_schema(frame: Option<&QrespFrame>) -> QrespResult<Option<FieldSchema>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(QrespFrame::Bulk(bytes)) => serde_json::from_slice(bytes)
                .map(Some)
                .map_err(|e| QrespError::Invalid(format!("field schema parse failed: {}", e))),
            Some(other) => Err(QrespError::Invalid(format!(
                "field schema must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_option_page_opts(page_opts: &Option<PageOpts>) -> QrespResult<QrespFrame> {
        match page_opts {
            Some(opts) => {
                let data = serde_json::to_vec(opts)
                    .map_err(|e| QrespError::Invalid(format!("page opts serialization failed: {}", e)))?;
                Ok(QrespFrame::Bulk(data))
            }
            None => Ok(QrespFrame::Null),
        }
    }

    fn decode_option_page_opts(frame: Option<&QrespFrame>) -> QrespResult<Option<PageOpts>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(QrespFrame::Bulk(bytes)) => serde_json::from_slice(bytes)
                .map(Some)
                .map_err(|e| QrespError::Invalid(format!("page opts parse failed: {}", e))),
            Some(other) => Err(QrespError::Invalid(format!(
                "page opts must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_option_page_result(result: &Option<PageResult<EntityId>>) -> QrespResult<QrespFrame> {
        match result {
            Some(res) => {
                let data = serde_json::to_vec(res)
                    .map_err(|e| QrespError::Invalid(format!("page result serialization failed: {}", e)))?;
                Ok(QrespFrame::Bulk(data))
            }
            None => Ok(QrespFrame::Null),
        }
    }

    fn decode_option_page_result(frame: Option<&QrespFrame>) -> QrespResult<Option<PageResult<EntityId>>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(QrespFrame::Bulk(bytes)) => serde_json::from_slice(bytes)
                .map(Some)
                .map_err(|e| QrespError::Invalid(format!("page result parse failed: {}", e))),
            Some(other) => Err(QrespError::Invalid(format!(
                "page result must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn encode_option_page_entity_type(result: &Option<PageResult<EntityType>>) -> QrespResult<QrespFrame> {
        match result {
            Some(res) => {
                let data = serde_json::to_vec(res)
                    .map_err(|e| QrespError::Invalid(format!("page result serialization failed: {}", e)))?;
                Ok(QrespFrame::Bulk(data))
            }
            None => Ok(QrespFrame::Null),
        }
    }

    fn decode_option_page_entity_type(frame: Option<&QrespFrame>) -> QrespResult<Option<PageResult<EntityType>>> {
        match frame {
            Some(QrespFrame::Null) | None => Ok(None),
            Some(QrespFrame::Bulk(bytes)) => serde_json::from_slice(bytes)
                .map(Some)
                .map_err(|e| QrespError::Invalid(format!("page result parse failed: {}", e))),
            Some(other) => Err(QrespError::Invalid(format!(
                "page result must be bulk, got {:?}",
                other
            ))),
        }
    }

    fn decode_push_condition(value: String) -> PushCondition {
        match value.as_str() {
            "always" => PushCondition::Always,
            "changes" => PushCondition::Changes,
            _ => PushCondition::Always,
        }
    }

    fn decode_adjust_behavior(value: String) -> AdjustBehavior {
        match value.as_str() {
            "add" => AdjustBehavior::Add,
            "subtract" => AdjustBehavior::Subtract,
            _ => AdjustBehavior::Set,
        }
    }

    fn bulk_str(text: &str) -> QrespFrame {
        QrespFrame::Bulk(text.as_bytes().to_vec())
    }

    fn encode_u64_as_integer(value: u64) -> QrespResult<QrespFrame> {
        let signed = i64::try_from(value)
            .map_err(|_| QrespError::Invalid(format!("value {} exceeds i64", value)))?;
        Ok(QrespFrame::Integer(signed))
    }

    fn decode_u64_from_integer(frame: &QrespFrame) -> QrespResult<u64> {
        match frame {
            QrespFrame::Integer(value) if *value >= 0 => Ok(*value as u64),
            other => Err(QrespError::Invalid(format!(
                "expected non-negative integer, got {:?}",
                other
            ))),
        }
    }

    fn ensure_len(items: &[QrespFrame], expected: usize, context: &str) -> QrespResult<()> {
        if items.len() < expected {
            return Err(QrespError::Invalid(format!(
                "{} expects at least {} arguments, got {}",
                context,
                expected,
                items.len()
            )));
        }
        Ok(())
    }

    fn map_from_entries(entries: Vec<(QrespFrame, QrespFrame)>) -> QrespResult<HashMap<String, QrespFrame>> {
        let mut map = HashMap::with_capacity(entries.len());
        for (key_frame, value_frame) in entries {
            let key = take_string(&key_frame)?;
            map.insert(key, value_frame);
        }
        Ok(map)
    }

    fn take_string(frame: &QrespFrame) -> QrespResult<String> {
        match frame {
            QrespFrame::Bulk(bytes) => String::from_utf8(bytes.clone())
                .map_err(|e| QrespError::Invalid(format!("invalid UTF-8: {}", e))),
            QrespFrame::Simple(text) => Ok(text.clone()),
            other => Err(QrespError::Invalid(format!(
                "expected string, got {:?}",
                other
            ))),
        }
    }

    fn require_string(map: &HashMap<String, QrespFrame>, key: &str) -> QrespResult<String> {
        let frame = require_frame(map, key)?;
        take_string(frame)
    }

    fn require_frame<'a>(map: &'a HashMap<String, QrespFrame>, key: &str) -> QrespResult<&'a QrespFrame> {
        map.get(key)
            .ok_or_else(|| QrespError::Invalid(format!("missing key '{}'", key)))
    }

    fn optional_frame<'a>(map: &'a HashMap<String, QrespFrame>, key: &str) -> QrespResult<Option<&'a QrespFrame>> {
        Ok(map.get(key))
    }

    fn optional_frame_owned(map: &HashMap<String, QrespFrame>, key: &str) -> QrespResult<Option<QrespFrame>> {
        Ok(map.get(key).cloned())
    }
}

pub fn encode_store_message(message: &crate::data::StoreMessage) -> Result<Vec<u8>> {
    let frame = store::encode_store_message(message)?;
    Ok(QrespCodec::encode(&frame))
}
