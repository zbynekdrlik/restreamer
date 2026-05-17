//! URL parsing and AMF connect-property construction for RTMP/RTMPS.

use std::io;

use rtmp::netconnection::writer::ConnectProperties;

use crate::PushError;

// -------------------------------------------------------------------------
// Scheme
// -------------------------------------------------------------------------

/// URL scheme of the upstream RTMP endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scheme {
    Rtmp,
    Rtmps,
}

// -------------------------------------------------------------------------
// URL helpers
// -------------------------------------------------------------------------

/// Build the `tcUrl` AMF property for `NetConnection.connect`.
///
/// Per libobs / ffmpeg convention, the port is omitted from `tcUrl` when
/// it equals the scheme default (443 for rtmps, 1935 for rtmp). Facebook
/// Live ingest validates `tcUrl` strictly and rejects publish with
/// "Invalid URL" when the literal `:443` port suffix appears (#215).
pub(crate) fn build_tc_url(scheme: Scheme, host: &str, port: u16, app: &str) -> String {
    let scheme_str = match scheme {
        Scheme::Rtmp => "rtmp",
        Scheme::Rtmps => "rtmps",
    };
    let default_port = match scheme {
        Scheme::Rtmp => 1935,
        Scheme::Rtmps => 443,
    };
    if port == default_port {
        format!("{scheme_str}://{host}/{app}")
    } else {
        format!("{scheme_str}://{host}:{port}/{app}")
    }
}

/// Construct the AMF `ConnectProperties` for `NetConnection.connect`.
///
/// Mirrors libobs `obs-outputs/rtmp-stream.c` values: `flashVer`, `fpad`,
/// `capabilities`, `audioCodecs`, `videoCodecs`, `videoFunction`,
/// `objectEncoding`. Adds `swfUrl` + `pageUrl` matching `tcUrl` because
/// Facebook Live validates these fields on some publish paths (#215).
pub(crate) fn build_connect_props(
    scheme: Scheme,
    host: &str,
    port: u16,
    app: &str,
) -> ConnectProperties {
    let tc_url = build_tc_url(scheme, host, port, app);
    let mut props = ConnectProperties::new_none();
    props.app = Some(app.to_string());
    props.pub_type = Some("nonprivate".to_string());
    props.flash_ver = Some("FMLE/3.0 (compatible; FMSc/1.0)".to_string());
    props.fpad = Some(false);
    props.capabilities = Some(239.0);
    props.audio_codecs = Some(3575.0);
    props.video_codecs = Some(252.0);
    props.video_function = Some(1.0);
    props.object_encoding = Some(0.0);
    props.swf_url = Some(tc_url.clone());
    props.page_url = Some(tc_url.clone());
    props.tc_url = Some(tc_url);
    props
}

pub(crate) fn parse_rtmp_url(
    url: &str,
) -> Result<(Scheme, String, u16, String, String), PushError> {
    let (scheme, rest, default_port) = if let Some(r) = url.strip_prefix("rtmps://") {
        (Scheme::Rtmps, r, 443u16)
    } else if let Some(r) = url.strip_prefix("rtmp://") {
        (Scheme::Rtmp, r, 1935u16)
    } else {
        return Err(bad_url("must start with rtmp:// or rtmps://", url));
    };

    let slash = rest
        .find('/')
        .ok_or_else(|| bad_url("missing /app/stream path", url))?;

    let authority = &rest[..slash];
    let path = &rest[slash + 1..];

    let (host, port) = if let Some(colon) = authority.rfind(':') {
        let h = &authority[..colon];
        let p: u16 = authority[colon + 1..]
            .parse()
            .map_err(|_| bad_url("invalid port number", url))?;
        (h.to_string(), p)
    } else {
        (authority.to_string(), default_port)
    };

    if host.is_empty() {
        return Err(bad_url("host is empty", url));
    }

    let slash2 = path
        .find('/')
        .ok_or_else(|| bad_url("path must contain /app/stream (two components)", url))?;

    let app = path[..slash2].to_string();
    let stream = path[slash2 + 1..].to_string();

    if app.is_empty() {
        return Err(bad_url("app name is empty", url));
    }
    if stream.is_empty() {
        return Err(bad_url("stream name is empty", url));
    }

    Ok((scheme, host, port, app, stream))
}

fn bad_url(reason: &str, url: &str) -> PushError {
    PushError::IoError(io::Error::other(format!("bad RTMP URL ({reason}): {url}")))
}

// -------------------------------------------------------------------------
// Unit tests
// -------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{Scheme, build_connect_props, build_tc_url, parse_rtmp_url};

    // --- URL parser tests ---------------------------------------------------

    #[test]
    fn parse_standard_rtmp_url() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmp://a.example.com/live/test").unwrap();
        assert_eq!(scheme, Scheme::Rtmp);
        assert_eq!(host, "a.example.com");
        assert_eq!(port, 1935);
        assert_eq!(app, "live");
        assert_eq!(stream, "test");
    }

    #[test]
    fn parse_rtmp_url_with_port() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmp://127.0.0.1:19350/live/mykey").unwrap();
        assert_eq!(scheme, Scheme::Rtmp);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19350);
        assert_eq!(app, "live");
        assert_eq!(stream, "mykey");
    }

    #[test]
    fn parse_standard_rtmps_url() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmps://live-api-s.facebook.com/rtmp/abc123").unwrap();
        assert_eq!(scheme, Scheme::Rtmps);
        assert_eq!(host, "live-api-s.facebook.com");
        assert_eq!(port, 443);
        assert_eq!(app, "rtmp");
        assert_eq!(stream, "abc123");
    }

    #[test]
    fn parse_rtmps_url_with_explicit_port() {
        let (scheme, host, port, app, stream) =
            parse_rtmp_url("rtmps://127.0.0.1:19443/live/test").unwrap();
        assert_eq!(scheme, Scheme::Rtmps);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19443);
        assert_eq!(app, "live");
        assert_eq!(stream, "test");
    }

    #[test]
    fn rejects_non_rtmp_scheme() {
        assert!(parse_rtmp_url("http://host/live/test").is_err());
    }

    #[test]
    fn rejects_missing_stream() {
        assert!(parse_rtmp_url("rtmp://host/live").is_err());
        assert!(parse_rtmp_url("rtmps://host/live").is_err());
    }

    #[test]
    fn rejects_empty_app() {
        assert!(parse_rtmp_url("rtmp://host//stream").is_err());
    }

    // --- tc_url builder tests -----------------------------------------------

    #[test]
    fn build_tc_url_omits_default_port_for_rtmps() {
        let url = build_tc_url(Scheme::Rtmps, "live-api-s.facebook.com", 443, "rtmp");
        assert_eq!(url, "rtmps://live-api-s.facebook.com/rtmp");
    }

    #[test]
    fn build_tc_url_omits_default_port_for_rtmp() {
        let url = build_tc_url(Scheme::Rtmp, "a.rtmp.youtube.com", 1935, "live2");
        assert_eq!(url, "rtmp://a.rtmp.youtube.com/live2");
    }

    #[test]
    fn build_tc_url_retains_custom_port_for_rtmp() {
        let url = build_tc_url(Scheme::Rtmp, "127.0.0.1", 19350, "live");
        assert_eq!(url, "rtmp://127.0.0.1:19350/live");
    }

    #[test]
    fn build_tc_url_retains_custom_port_for_rtmps() {
        let url = build_tc_url(Scheme::Rtmps, "127.0.0.1", 19443, "live");
        assert_eq!(url, "rtmps://127.0.0.1:19443/live");
    }

    // --- build_connect_props tests ------------------------------------------

    #[test]
    fn connect_props_for_fb_sets_swf_url_and_page_url_without_port() {
        let props = build_connect_props(Scheme::Rtmps, "live-api-s.facebook.com", 443, "rtmp");
        assert_eq!(props.app.as_deref(), Some("rtmp"));
        assert_eq!(
            props.tc_url.as_deref(),
            Some("rtmps://live-api-s.facebook.com/rtmp")
        );
        assert_eq!(
            props.swf_url.as_deref(),
            Some("rtmps://live-api-s.facebook.com/rtmp")
        );
        assert_eq!(
            props.page_url.as_deref(),
            Some("rtmps://live-api-s.facebook.com/rtmp")
        );
    }

    #[test]
    fn connect_props_preserves_legacy_fields() {
        let props = build_connect_props(Scheme::Rtmp, "a.rtmp.youtube.com", 1935, "live2");
        assert_eq!(
            props.flash_ver.as_deref(),
            Some("FMLE/3.0 (compatible; FMSc/1.0)")
        );
        assert_eq!(props.fpad, Some(false));
        assert_eq!(props.capabilities, Some(239.0));
        assert_eq!(props.audio_codecs, Some(3575.0));
        assert_eq!(props.video_codecs, Some(252.0));
        assert_eq!(props.video_function, Some(1.0));
        assert_eq!(props.object_encoding, Some(0.0));
        assert_eq!(props.pub_type.as_deref(), Some("nonprivate"));
        assert_eq!(props.app.as_deref(), Some("live2"));
        assert_eq!(
            props.tc_url.as_deref(),
            Some("rtmp://a.rtmp.youtube.com/live2")
        );
    }
}
