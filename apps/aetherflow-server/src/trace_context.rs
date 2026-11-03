//! W3C Trace Context extraction and injection for HTTP boundaries.
//!
//! Only the standard traceparent/tracestate fields are propagated. Baggage,
//! arbitrary headers, and request metadata are deliberately excluded.

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;

struct HeaderMapExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(HeaderName::as_str).collect()
    }
}

struct HeaderMapInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(&value),
        ) else {
            return;
        };
        self.0.insert(name, value);
    }
}

pub(crate) fn extract_trace_context(headers: &HeaderMap) -> Context {
    TraceContextPropagator::new().extract(&HeaderMapExtractor(headers))
}

#[allow(dead_code)]
pub(crate) fn inject_trace_context(context: &Context, headers: &mut HeaderMap) {
    TraceContextPropagator::new().inject_context(context, &mut HeaderMapInjector(headers));
}

#[cfg(test)]
mod tests {
    use super::{extract_trace_context, inject_trace_context};
    use axum::http::{HeaderMap, HeaderValue};
    use opentelemetry::trace::TraceContextExt;

    const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn extracts_valid_w3c_trace_context() {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", HeaderValue::from_static(TRACEPARENT));
        headers.insert("tracestate", HeaderValue::from_static("vendor=value"));

        let context = extract_trace_context(&headers);
        let span_context = context.span().span_context();
        assert!(span_context.is_valid());
        assert!(span_context.is_sampled());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
        assert_eq!(span_context.trace_state().header(), "vendor=value");
    }

    #[test]
    fn rejects_invalid_w3c_trace_context_and_creates_no_remote_parent() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            HeaderValue::from_static("00-00000000000000000000000000000000-0000000000000000-01"),
        );

        let context = extract_trace_context(&headers);
        assert!(!context.span().span_context().is_valid());
    }

    #[test]
    fn injects_only_w3c_trace_context_headers() {
        let mut incoming = HeaderMap::new();
        incoming.insert("traceparent", HeaderValue::from_static(TRACEPARENT));
        incoming.insert("tracestate", HeaderValue::from_static("vendor=value"));
        let context = extract_trace_context(&incoming);

        let mut outgoing = HeaderMap::new();
        inject_trace_context(&context, &mut outgoing);

        assert_eq!(outgoing.get("traceparent").unwrap(), TRACEPARENT);
        assert_eq!(outgoing.get("tracestate").unwrap(), "vendor=value");
        assert_eq!(outgoing.len(), 2);
    }
}
