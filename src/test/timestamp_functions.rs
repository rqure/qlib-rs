#[cfg(test)]
mod tests {
    use crate::*;
    use crate::data::{nanos_to_timestamp, secs_to_timestamp, millis_to_timestamp, micros_to_timestamp};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn test_nanos_to_timestamp() {
        let nanos = 1625097600_000_000_000u64; // July 1, 2021 00:00:00 UTC in nanoseconds
        let timestamp = nanos_to_timestamp(nanos);
        
        let expected = UNIX_EPOCH + Duration::from_nanos(nanos);
        assert_eq!(timestamp, expected);
        
        // Test with zero
        let zero_timestamp = nanos_to_timestamp(0);
        assert_eq!(zero_timestamp, UNIX_EPOCH);
        
        // Test with current time
        let current_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let current_timestamp = nanos_to_timestamp(current_nanos);
        
        // Should be within a reasonable range of current time
        let now = SystemTime::now();
        let diff = now.duration_since(current_timestamp).unwrap_or_else(|_| current_timestamp.duration_since(now).unwrap());
        assert!(diff < Duration::from_secs(1)); // Should be within 1 second
    }

    #[test]
    fn test_secs_to_timestamp() {
        let secs = 1625097600u64; // July 1, 2021 00:00:00 UTC
        let timestamp = secs_to_timestamp(secs);
        
        let expected = UNIX_EPOCH + Duration::from_secs(secs);
        assert_eq!(timestamp, expected);
        
        // Test with zero
        let zero_timestamp = secs_to_timestamp(0);
        assert_eq!(zero_timestamp, UNIX_EPOCH);
        
        // Test with a known date
        let known_date_secs = 946684800u64; // January 1, 2000 00:00:00 UTC
        let known_timestamp = secs_to_timestamp(known_date_secs);
        let expected_known = UNIX_EPOCH + Duration::from_secs(known_date_secs);
        assert_eq!(known_timestamp, expected_known);
    }

    #[test]
    fn test_millis_to_timestamp() {
        let millis = 1625097600_000u64; // July 1, 2021 00:00:00 UTC in milliseconds
        let timestamp = millis_to_timestamp(millis);
        
        let expected = UNIX_EPOCH + Duration::from_millis(millis);
        assert_eq!(timestamp, expected);
        
        // Test with zero
        let zero_timestamp = millis_to_timestamp(0);
        assert_eq!(zero_timestamp, UNIX_EPOCH);
        
        // Test with sub-second precision
        let millis_with_fraction = 1625097600_123u64; // 123 milliseconds past the second
        let timestamp_with_fraction = millis_to_timestamp(millis_with_fraction);
        let expected_with_fraction = UNIX_EPOCH + Duration::from_millis(millis_with_fraction);
        assert_eq!(timestamp_with_fraction, expected_with_fraction);
    }

    #[test]
    fn test_micros_to_timestamp() {
        let micros = 1625097600_000_000u64; // July 1, 2021 00:00:00 UTC in microseconds
        let timestamp = micros_to_timestamp(micros);
        
        let expected = UNIX_EPOCH + Duration::from_micros(micros);
        assert_eq!(timestamp, expected);
        
        // Test with zero
        let zero_timestamp = micros_to_timestamp(0);
        assert_eq!(zero_timestamp, UNIX_EPOCH);
        
        // Test with sub-millisecond precision
        let micros_with_fraction = 1625097600_123_456u64; // 123.456 milliseconds past the second
        let timestamp_with_fraction = micros_to_timestamp(micros_with_fraction);
        let expected_with_fraction = UNIX_EPOCH + Duration::from_micros(micros_with_fraction);
        assert_eq!(timestamp_with_fraction, expected_with_fraction);
    }

    #[test]
    fn test_timestamp_conversions_consistency() {
        // Test that different time unit conversions produce consistent results
        let base_secs = 1625097600u64;
        
        let from_secs = secs_to_timestamp(base_secs);
        let from_millis = millis_to_timestamp(base_secs * 1000);
        let from_micros = micros_to_timestamp(base_secs * 1_000_000);
        let from_nanos = nanos_to_timestamp(base_secs * 1_000_000_000);
        
        assert_eq!(from_secs, from_millis);
        assert_eq!(from_millis, from_micros);
        assert_eq!(from_micros, from_nanos);
    }

    #[test]
    fn test_now_and_epoch() {
        let current_time = now();
        let epoch_time = epoch();
        
        // Current time should be after epoch
        assert!(current_time > epoch_time);
        
        // Epoch should be UNIX_EPOCH
        assert_eq!(epoch_time, UNIX_EPOCH);
        
        // Now should be within a reasonable range of system time
        let system_now = SystemTime::now();
        let diff = system_now.duration_since(current_time)
            .unwrap_or_else(|_| current_time.duration_since(system_now).unwrap());
        assert!(diff < Duration::from_secs(1));
    }

    #[test]
    fn test_timestamp_precision_boundaries() {
        // Test edge cases for precision boundaries
        
        // Maximum values that fit in different time units
        let max_safe_secs = u32::MAX as u64; // Still safe for timestamp operations
        let max_safe_millis = max_safe_secs * 1000;
        let max_safe_micros = max_safe_secs * 1_000_000;
        let max_safe_nanos = max_safe_secs * 1_000_000_000;
        
        let ts_secs = secs_to_timestamp(max_safe_secs);
        let ts_millis = millis_to_timestamp(max_safe_millis);
        let ts_micros = micros_to_timestamp(max_safe_micros);
        let ts_nanos = nanos_to_timestamp(max_safe_nanos);
        
        // All should be valid timestamps
        assert!(ts_secs > UNIX_EPOCH);
        assert!(ts_millis > UNIX_EPOCH);
        assert!(ts_micros > UNIX_EPOCH);
        assert!(ts_nanos > UNIX_EPOCH);
        
        // Higher precision should equal or exceed lower precision for same base time
        assert_eq!(ts_secs, ts_millis);
        assert_eq!(ts_millis, ts_micros);
        assert_eq!(ts_micros, ts_nanos);
    }
}
