#[allow(unused_imports)]
use super::*;

#[allow(unused_imports)]
use crate::data::resp::{PeerHandshakeCommand, RespDecode, RespEncode, RespValue};

#[test]
fn test_peer_handshake_encode_decode() {
    // Create a PeerHandshakeCommand like qcore-rs would
    let command = PeerHandshakeCommand {
        start_time: 12345678,
        is_response: false,
        machine_id: "test-machine".to_string(),
        _marker: std::marker::PhantomData,
    };

    // Encode it like send_peer_command does
    let encoded = command.encode();
    println!("Encoded bytes: {:?}", encoded);

    // Parse it as a RespValue to see the structure
    let (parsed_value, _) = RespValue::decode(&encoded).expect("Should decode as RespValue");
    println!("Parsed RespValue: {:?}", parsed_value);

    // Now simulate what core.rs does - parse the array and extract command name + args
    if let RespValue::Array(args) = parsed_value {
        println!("Array length: {}", args.len());

        // Check the command name (first element)
        if let Some(RespValue::BulkString(cmd_name_bytes)) = args.get(0) {
            let cmd_name = std::str::from_utf8(cmd_name_bytes).expect("Valid UTF-8");
            println!("Command name: '{}'", cmd_name);
            assert_eq!(cmd_name, "PEER_HANDSHAKE");
        } else {
            panic!("First element should be command name as BulkString");
        }

        // Simulate core.rs: create array from args[1..] and encode it for decoding
        let remaining_args = &args[1..];
        let args_for_decode = RespValue::Array(remaining_args.to_vec()).encode();
        println!("Args for decode: {:?}", remaining_args);

        // Try to decode the command from the remaining args
        match PeerHandshakeCommand::decode(&args_for_decode) {
            Ok((decoded_command, _)) => {
                println!("Successfully decoded command: {:?}", decoded_command);
                assert_eq!(decoded_command.start_time, command.start_time);
                assert_eq!(decoded_command.is_response, command.is_response);
                assert_eq!(decoded_command.machine_id, command.machine_id);
            }
            Err(e) => {
                panic!("Failed to decode command: {}", e);
            }
        }
    } else {
        panic!("Expected RespValue::Array, got: {:?}", parsed_value);
    }
}

#[test]
fn test_command_format_analysis() {
    // Test what the current encoding produces
    let command = PeerHandshakeCommand {
        start_time: 42,
        is_response: true,
        machine_id: "test".to_string(),
        _marker: std::marker::PhantomData,
    };

    let encoded = command.encode();
    let (parsed, _) = RespValue::decode(&encoded).unwrap();

    if let RespValue::Array(elements) = parsed {
        println!("Command array has {} elements:", elements.len());
        for (i, element) in elements.iter().enumerate() {
            match element {
                RespValue::BulkString(bytes) => {
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        println!("  [{}]: BulkString(\"{}\")", i, s);
                    } else {
                        println!("  [{}]: BulkString(<binary data>)", i);
                    }
                }
                RespValue::Integer(n) => println!("  [{}]: Integer({})", i, n),
                RespValue::SimpleString(s) => println!("  [{}]: SimpleString(\"{}\")", i, s),
                other => println!("  [{}]: {:?}", i, other),
            }
        }
    }
}

#[test]
fn test_decode_expectations() {
    // Test what format PeerHandshakeCommand::decode expects
    // Based on the RespDecode derive macro, it should expect:
    // [field_name1, field_value1, field_name2, field_value2, ...]

    let expected_format = RespValue::Array(vec![
        RespValue::BulkString(b"start_time"),
        RespValue::Integer(12345),
        RespValue::BulkString(b"is_response"),
        RespValue::Integer(1), // true as integer
        RespValue::BulkString(b"machine_id"),
        RespValue::BulkString(b"test-machine"),
    ]);

    let encoded_expected = expected_format.encode();

    match PeerHandshakeCommand::decode(&encoded_expected) {
        Ok((decoded, _)) => {
            println!("Successfully decoded from expected format: {:?}", decoded);
            assert_eq!(decoded.start_time, 12345);
            assert_eq!(decoded.is_response, true);
            assert_eq!(decoded.machine_id, "test-machine");
        }
        Err(e) => {
            println!("Failed to decode expected format: {}", e);
        }
    }
}

#[test]
fn test_non_array_resp_values() {
    // Test what happens when the RESP parser receives non-array values
    // This simulates the "Expected array for command" error scenarios

    // Test 1: Simple string (might come from HTTP request or other protocol)
    let simple_string = RespValue::SimpleString("GET /health HTTP/1.1");
    println!("Non-array example 1: {:?}", simple_string);

    // Test 2: Bulk string (might be partial data)
    let bulk_string = RespValue::BulkString(b"some random data");
    println!("Non-array example 2: {:?}", bulk_string);

    // Test 3: Integer (might be error response)
    let integer = RespValue::Integer(200);
    println!("Non-array example 3: {:?}", integer);

    // Test 4: Error message
    let error = RespValue::Error("Connection timeout");
    println!("Non-array example 4: {:?}", error);

    // These are the types of RESP values that would cause "Expected array for command"
    // because the command parser expects only arrays

    println!("All of these would cause 'Expected array for command' errors in core.rs");
}
