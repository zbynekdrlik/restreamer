//! EndpointReader -- replaces the consumer_task hot loop. Reads chunks
//! from local disk and pushes via RtmpPusher. No S3 calls in hot path.

pub struct EndpointReader {
    _placeholder: (),
}
