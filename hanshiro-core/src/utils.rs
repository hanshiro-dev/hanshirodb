//! # Common Utilities
//!
//! Utility functions and helpers used throughout HanshiroDB.

use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;

/// Generate unique timestamp-based ID
pub fn generate_timestamp_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos() as u64
}

/// Format bytes in human readable format
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    
    if bytes == 0 {
        return "0 B".to_string();
    }
    
    let bytes_f64 = bytes as f64;
    let exp = (bytes_f64.log2() / 10.0).floor() as usize;
    let unit_index = exp.min(UNITS.len() - 1);
    let size = bytes_f64 / (1024_f64).powi(unit_index as i32);
    
    if size >= 100.0 {
        format!("{:.0} {}", size, UNITS[unit_index])
    } else if size >= 10.0 {
        format!("{:.1} {}", size, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

/// Clamp value to range
pub fn clamp<T: PartialOrd>(value: T, min: T, max: T) -> T {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

/// Round up to next power of 2
pub fn next_power_of_two(n: u64) -> u64 {
    if n == 0 {
        1
    } else {
        (n - 1).next_power_of_two()
    }
}

/// Check if value is power of 2
pub fn is_power_of_two(n: u64) -> bool {
    n != 0 && (n & (n - 1)) == 0
}

/// Align value to boundary
pub fn align_to(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) / alignment * alignment
}

/// Bounded channel for backpressure
pub mod bounded_channel {
    use tokio::sync::{mpsc, Semaphore};
    use std::sync::Arc;
    
    pub struct BoundedSender<T> {
        tx: mpsc::UnboundedSender<T>,
        semaphore: Arc<Semaphore>,
    }
    
    pub struct BoundedReceiver<T> {
        rx: mpsc::UnboundedReceiver<T>,
        semaphore: Arc<Semaphore>,
    }
    
    impl<T> BoundedSender<T> {
        pub async fn send(&self, value: T) -> Result<(), String> {
            let permit = self.semaphore.acquire().await
                .map_err(|e| e.to_string())?;
            
            self.tx.send(value)
                .map_err(|e| e.to_string())?;
                
            // Permit is dropped here, releasing slot
            drop(permit);
            Ok(())
        }
    }
    
    impl<T> BoundedReceiver<T> {
        pub async fn recv(&mut self) -> Option<T> {
            let value = self.rx.recv().await?;
            self.semaphore.add_permits(1);
            Some(value)
        }
    }
    
    pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let semaphore = Arc::new(Semaphore::new(capacity));
        
        (
            BoundedSender {
                tx,
                semaphore: semaphore.clone(),
            },
            BoundedReceiver {
                rx,
                semaphore,
            },
        )
    }
}

/// Retry with exponential backoff
pub async fn retry_with_backoff<T, E, F, Fut>(
    mut f: F,
    max_retries: u32,
    initial_delay_ms: u64,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut delay = initial_delay_ms;
    let mut attempts = 0;
    
    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(e);
                }
                
                tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                delay = (delay * 2).min(60_000); // Cap at 1 minute
            }
        }
    }
}

/// Simple rate limiter
pub struct RateLimiter {
    capacity: u32,
    tokens: tokio::sync::Mutex<u32>,
    refill_interval: tokio::time::Duration,
}

impl RateLimiter {
    pub fn new(capacity: u32, refill_interval: tokio::time::Duration) -> Self {
        Self {
            capacity,
            tokens: tokio::sync::Mutex::new(capacity),
            refill_interval,
        }
    }
    
    pub async fn acquire(&self) -> Result<(), &'static str> {
        let mut tokens = self.tokens.lock().await;
        if *tokens > 0 {
            *tokens -= 1;
            Ok(())
        } else {
            Err("Rate limit exceeded")
        }
    }
    
    pub async fn start_refill(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.refill_interval);
        loop {
            interval.tick().await;
            let mut tokens = self.tokens.lock().await;
            *tokens = self.capacity;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }
    
    #[test]
    fn test_power_of_two() {
        assert_eq!(next_power_of_two(0), 1);
        assert_eq!(next_power_of_two(1), 1);
        assert_eq!(next_power_of_two(2), 2);
        assert_eq!(next_power_of_two(3), 4);
        assert_eq!(next_power_of_two(5), 8);
        
        assert!(is_power_of_two(1));
        assert!(is_power_of_two(2));
        assert!(is_power_of_two(4));
        assert!(!is_power_of_two(3));
        assert!(!is_power_of_two(0));
    }
    
    #[test]
    fn test_align() {
        assert_eq!(align_to(0, 8), 0);
        assert_eq!(align_to(1, 8), 8);
        assert_eq!(align_to(7, 8), 8);
        assert_eq!(align_to(8, 8), 8);
        assert_eq!(align_to(9, 8), 16);
    }
}