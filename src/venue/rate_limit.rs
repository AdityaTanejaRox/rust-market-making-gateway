use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct TokenBucket {
    capacity: u32,
    tokens: u32,
    refill_per_second: u32,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_per_second: u32) -> Self {
        Self {
            capacity,
            tokens: capacity,
            refill_per_second,
            last_refill: Instant::now(),
        }
    }

    pub fn try_acquire(&mut self) -> bool {
        self.refill();

        if self.tokens == 0 {
            return false;
        }

        self.tokens -= 1;
        true
    }

    fn refill(&mut self) {
        let elapsed = self.last_refill.elapsed();

        if elapsed < Duration::from_secs(1) {
            return;
        }

        let elapsed_secs = elapsed.as_secs() as u32;
        let refill_amount = elapsed_secs.saturating_mul(self.refill_per_second);

        self.tokens = self.capacity.min(self.tokens.saturating_add(refill_amount));
        self.last_refill = Instant::now();
    }

    #[cfg(test)]
    fn force_refill_after_seconds(&mut self, seconds: u64) {
        self.last_refill = Instant::now() - Duration::from_secs(seconds);
    }

    #[cfg(test)]
    fn tokens(&self) -> u32 {
        self.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_requests_until_bucket_empty() {
        let mut bucket = TokenBucket::new(2, 2);

        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn refills_after_one_second() {
        let mut bucket = TokenBucket::new(2, 2);

        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(!bucket.try_acquire());

        bucket.force_refill_after_seconds(1);

        assert!(bucket.try_acquire());
        assert_eq!(bucket.tokens(), 1);
    }

    #[test]
    fn refill_does_not_exceed_capacity() {
        let mut bucket = TokenBucket::new(3, 10);

        assert!(bucket.try_acquire());
        bucket.force_refill_after_seconds(10);
        assert!(bucket.try_acquire());

        assert!(bucket.tokens() <= 2);
    }
}