# rs-rtmp-push: rtmps:// TLS support — Design

**Issue:** #156
**Branch:** dev (currently dev = main = v0.3.74; this PR bumps to 0.3.75)
**Rollout context:** PR 2 prerequisite of the #103 rs-rtmp-push migration. Once this lands, the operator can flip Facebook / Vimeo / Instagram endpoints to `pusher='rust'`.

---

## 1. Problem

`rs_rtmp_push::session::Session::connect` opens plain TCP via `tokio::net::TcpSocket → TcpStream`. Facebook (`rtmps://live-api-s.facebook.com:443`), Vimeo (`rtmps://rtmp-global.cloud.vimeo.com:443`), and Instagram (`rtmps://live-upload.instagram.com:443`) wrap RTMP in TLS. Plain TCP into a TLS listener silently absorbs bytes; the pusher hangs at `chunks_processed=0` with `cache_delay_secs` growing past the threshold and no audit error.

This was confirmed empirically on 2026-04-30 when the operator flipped `FB-Zbynek` to `pusher='rust'`: cache hit 179s and kept growing while `chunks_processed` stayed at 0.

`crates/rs-delivery/src/endpoint_task.rs::build_rtmp_url` already produces `rtmps://...` URLs for Facebook / Vimeo / Instagram. The only gap is the client TLS wrap inside `rs-rtmp-push`.

## 2. Scope

In:
- Detect `rtmp://` vs `rtmps://` in `Session::connect`.
- For `rtmps://`, wrap the connected `TcpStream` in `tokio_rustls::client::TlsStream<TcpStream>` before constructing the `TNetIO` adapter.
- New `crates/rs-rtmp-push/src/tls.rs` with `TlsIO` (an `impl TNetIO` over `Framed<TlsStream<TcpStream>, BytesCodec>`) and a `connect_tls` helper.
- Workspace deps: `tokio-rustls`, `rustls`, `webpki-roots` for production; `rcgen` as a `[dev-dependencies]` of `rs-rtmp-push`.
- Integration test against a self-signed TLS RTMP server (xiu `RtmpServer` behind `tokio_rustls::TlsAcceptor`).

Out:
- mTLS / client certificates (no provider requires them).
- Custom CA bundles or pinning (use Mozilla roots via `webpki-roots`).
- ALPN negotiation (FB silent on ALPN, YT accepts no advertise — leave `alpn_protocols` empty).
- Flipping any production endpoint to `pusher='rust'` for `rtmps://` providers — that is PR 2 of the rollout, gated on issue #159 (4-h soak).

## 3. Architecture

`bytesio::TcpIO` is `Framed<TcpStream, BytesCodec>` and implements `TNetIO`. `TlsStream<TcpStream>` (from `tokio_rustls::client`) implements `AsyncRead + AsyncWrite + Send + Sync` and therefore composes into `Framed<_, BytesCodec>` the same way. We add `TlsIO` as a parallel `TNetIO` implementation:

```
Session::connect(url)
  ├── parse_rtmp_url(url) -> (scheme, host, port, app, stream)
  ├── tcp_stream = connect_tcp(host, port, sndbuf=4MB, nodelay=true)
  ├── io: Box<dyn TNetIO> = match scheme:
  │     "rtmp"  => Box::new(TcpIO::new(tcp_stream))
  │     "rtmps" => Box::new(connect_tls(tcp_stream, host).await?)
  └── negotiate(io, ...) -> Session     (unchanged: handshake, connect, createStream, publish)
```

The negotiate / wait_for / read_loop machinery already operates on `Box<dyn TNetIO + Send + Sync>` and is therefore transparent to the wire encryption.

## 4. Files & ownership

| File | Action | Notes |
|---|---|---|
| `crates/rs-rtmp-push/src/tls.rs` | **new** (~150 lines) | `TlsIO` struct + `impl TNetIO`, `connect_tls()`, lazy `ClientConfig` (webpki-roots), `connect_tls_with_config()` test-only entry point. |
| `crates/rs-rtmp-push/src/lib.rs` | edit | `pub mod tls;` |
| `crates/rs-rtmp-push/src/session.rs` | edit (~ +30 / -20 lines) | Extract IO construction into `build_io(scheme, tcp, host)`; add `Session::connect_with_tls_config` test-only constructor. Net file size ≈ 962, under the 1000-line cap. |
| `crates/rs-rtmp-push/src/session.rs::parse_rtmp_url` | edit | Accept `rtmps://` (default port 443) alongside `rtmp://` (default port 1935). Return `Scheme` enum. |
| `crates/rs-rtmp-push/Cargo.toml` | edit | Add `tokio-rustls`, `rustls`, `webpki-roots` to `[dependencies]`; `rcgen` to `[dev-dependencies]`. |
| `Cargo.toml` (workspace root) | edit | Add the three production deps to `[workspace.dependencies]`. |
| `crates/rs-rtmp-push/tests/local_tls_loopback.rs` | **new** | Self-signed-cert loopback regression test. |

session.rs currently sits at 952 lines. The split keeps it under 1000 without forcing a separate `negotiate.rs` move; that broader split is deferred to a future PR if needed.

## 5. Dependencies

```toml
# workspace [workspace.dependencies]
tokio-rustls = "0.26"
rustls       = { version = "0.23", default-features = false, features = ["std", "tls12", "tls13", "ring"] }
webpki-roots = "0.26"
```

Rationale for `rustls` features:
- `default-features = false` excludes `aws-lc-rs`, which pulls in C code and contradicts the pure-Rust monorepo principle.
- `ring` provides the crypto provider.
- `tls12 + tls13` covers all current providers (FB, YT, Vimeo, IG all support TLS 1.2; YT and modern FB also support 1.3).

```toml
# crates/rs-rtmp-push/Cargo.toml [dev-dependencies]
rcgen = "0.13"
```

`rcgen` generates a self-signed cert + private key in pure Rust; no `openssl` required.

## 6. Root store

`webpki-roots` (Mozilla CA bundle compiled in). All four target providers (FB, YT, Vimeo, IG) use public CAs. `rustls-native-certs` was rejected:
- adds an OS dependency on each Windows / Linux host;
- lookup is slow on Windows;
- saves only marginal binary size.

The lazy `ClientConfig` is built once per process via `OnceLock`:

```rust
static TLS_CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
fn tls_config() -> Arc<ClientConfig> {
    TLS_CONFIG.get_or_init(|| {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    }).clone()
}
```

## 7. ALPN & SNI

- ALPN: not advertised (`alpn_protocols` left empty).
- SNI: the `host` parsed from the URL, validated as a `ServerName` via `rustls_pki_types::ServerName::try_from(host.as_str())`.

## 8. TCP socket options

`TCP_NODELAY = true` and `SO_SNDBUF = 4 MB` are set on the raw `tokio::net::TcpSocket` BEFORE the `connect()` call. The TLS wrap operates on the connected `TcpStream`; socket options are unaffected by encryption layer.

## 9. Behavior preservation

The `rtmp://` path is byte-identical to today: same `TcpSocket` setup, same `TcpIO::new(stream)` construction, same `negotiate(...)` call, same `read_loop`. The TLS branch is dead code unless `scheme == "rtmps"`. Existing endpoints with `pusher='rust'` (only YouTube `rtmp://a.rtmp.youtube.com:1935` ships in production today) are unaffected.

## 10. Error handling

A new variant `PushError::TlsHandshakeFailed(String)` covers:
- TLS handshake errors (`tokio_rustls::rustls::Error`)
- Invalid SNI (host fails `ServerName::try_from`)

Existing `PushError::HandshakeFailed(io::Error)` remains for the underlying TCP connect.

## 11. Testing

### 11.1 Unit (in `tls.rs`)

- `parse_rtmps_url_default_port_443`
- `parse_rtmp_url_default_port_1935` (regression — must still pass)
- `parse_rtmps_with_explicit_port`

### 11.2 Integration: `tests/local_tls_loopback.rs`

xiu's `rtmp::session::server_session::ServerSession::new` accepts `TcpStream` concretely; it cannot consume a `TlsStream`. The test harness uses a **TLS bridge** rather than wrapping xiu directly:

1. Spawn the existing `spawn_recording_xiu_server()` helper on plain `127.0.0.1:<plain_port>` — same `StreamsHub` + recording subscriber the existing tests use.
2. Generate a self-signed cert via `rcgen::generate_simple_self_signed(["127.0.0.1".to_string()])`.
3. Build a `tokio_rustls::TlsAcceptor` from the cert chain + private key.
4. Bind a TLS listener on `127.0.0.1:<tls_port>`. For each TLS accept, perform `acceptor.accept(tcp).await`, then `TcpStream::connect(plain_port)` and `tokio::io::copy_bidirectional(tls_stream, plain_stream)`. This bridges TLS-decrypted bytes through to xiu over loopback plain TCP without modifying xiu.
5. Test client uses `Session::connect_with_tls_config(rtmps_url, 5000, test_cfg)`. `test_cfg` is a `ClientConfig` whose `RootCertStore` contains only the self-signed cert from step 2 — no webpki-roots involvement.
6. Push a small canned FLV (one AAC sequence header + one AVC sequence header + a few media tags) through the session.
7. Assert the recording subscriber captured the expected `BroadcastEvent::Publish` and at least one media frame, with byte-identical body SHA-256 to the source FLV (parity check with existing `media_payload_byte_identical_to_source` test).

The production `Session::connect` consumes the lazy webpki-roots config; the test entry point allows injecting the test CA without touching production.

The bridge harness lives in `tests/common/mod.rs` as `spawn_recording_xiu_server_tls()`. It returns `(rtmps_url, recorded_tags, ca_cert_der, server_handle, sub_ready_rx)` — same shape as the plain helper, plus the CA cert the client must trust.

### 11.3 Mutation testing

`rs-rtmp-push::tls` MUST NOT be added to `--exclude-re` in `.github/workflows/ci.yml` (per spec §7.6 of #103 — every `rs-rtmp-push` module participates in mutation testing from day 1).

## 12. CI changes

None required for this PR. The new test runs under the existing `Test (ubuntu-latest)` and `Test (windows-latest)` jobs. The mutation-testing matrix already covers `rs-rtmp-push`.

## 13. Acceptance

Per issue #156:

1. **Self-signed loopback test green** on Linux + Windows runners.
2. **Mutation score** for `crates/rs-rtmp-push/src/tls.rs` ≥ baseline established by the loopback test (no surviving mutants on the connect / configure / SNI paths).
3. **Existing rust-pusher path unchanged**: `Frontend E2E (Playwright)` and `E2E Streaming Test` and `E2E OBS-to-YouTube Test` all green (proves YT_RTMP `rtmp://` path is byte-identical to today).

End-to-end soak with FB on rust pusher (issue #159) is **not** part of this PR's acceptance — that runs after this PR ships, against a production endpoint flip on dev only.

## 14. Risks

- `rustls` 0.23 + `tokio-rustls` 0.26 ABI compatibility: pinned via workspace exact versions; `tokio-rustls` 0.26 re-exports `rustls` 0.23.
- `webpki-roots` 0.26 vs 0.27: 0.27 changes `TLS_SERVER_ROOTS` API. Pin 0.26 explicitly.
- `rcgen` and `rustls` share `rustls-pki-types` — must agree on version to avoid type-confusion build errors.
- Build on stream.lan (Windows): all three crates compile cleanly under MSVC; no C deps required because we use the `ring` crypto provider.

## 15. Plan handoff

Implementation plan: `docs/superpowers/plans/2026-05-01-rs-rtmp-push-rtmps-tls.md` (next step).

Tasks: 7 total. Tasks 1-6 dispatched via `superpowers:subagent-driven-development`. Task 7 (push, monitor CI, PR) is orchestrator-only.
