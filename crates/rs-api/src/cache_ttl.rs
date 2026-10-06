//! The freshness rule of the API's in-process TTL caches (the Facebook
//! ingest-health cache and the OAuth stream-key map, #367).

use std::time::Duration;

/// `true` while a cache entry of age `age` may still be served: strictly
/// younger than `ttl`. At exactly `ttl` the entry is stale and is refetched.
pub(crate) fn is_fresh(age: Duration, ttl: Duration) -> bool {
    age < ttl
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_only_strictly_before_the_ttl() {
        let ttl = Duration::from_secs(60);
        assert!(is_fresh(Duration::ZERO, ttl));
        assert!(is_fresh(Duration::from_millis(59_999), ttl));
        assert!(!is_fresh(ttl, ttl), "an entry exactly ttl old is stale");
        assert!(!is_fresh(Duration::from_secs(61), ttl));
    }
}
