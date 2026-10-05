# Versioning OTLP Profiles

Nayef Ghattas <nayef.ghattas@datadoghq.com> • July 2026 • Specification change approved in [opentelemetry-proto#857](https://github.com/open-telemetry/opentelemetry-proto/pull/857)

*This document is shared publicly with the OpenTelemetry community.*

This document preserves the context and tradeoffs behind the approved [OTLP Profiles versioning specification change](https://github.com/open-telemetry/opentelemetry-proto/blob/8c881680d50297009b1855f688dcfb17429d63b1/docs/specification.md#profiles-development-version).

## Summary

OTLP Profiles will continue to use `v1development` while incompatible Alpha and Beta changes are still allowed.

The mechanism uses a temporary request metadata key, `otlp-profiles-development-version`, so servers can reject unsupported formats before decoding. Missing metadata means revision `1`. The key applies to OTLP/HTTP and OTLP/gRPC and is retired when Profiles move to `v1`.

## Motivation

OTLP Profiles is currently Alpha and preparing for Beta. The [Beta roadmap](https://github.com/open-telemetry/sig-profiling/issues/117) includes at least one [incompatible data model change](https://github.com/open-telemetry/opentelemetry-proto/pull/786), and others may be needed before stability. This is allowed by the [OpenTelemetry maturity definitions](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/0232-maturity-of-otel.md).

Profiles will meanwhile continue to use the same `v1development` package, gRPC service, and `/v1development/profiles` endpoint until release candidate, following the decision in [sig-profiling#70](https://github.com/open-telemetry/sig-profiling/issues/70) and [opentelemetry-proto#771](https://github.com/open-telemetry/opentelemetry-proto/pull/771).

Because all development versions share the same service and endpoint, OTLP Profiles servers cannot tell which Profiles format they are receiving before decoding it. Protobuf decoding can still succeed for incompatible formats while silently dropping information. For example, the change above moves original-payload information to fields that older servers ignore. Other changes could instead cause decoding to fail or data to be interpreted incorrectly.

The mechanism adds a version identifier outside the payload to each OTLP Profiles request. An OTLP Profiles server checks the identifier before decoding and rejects the request if it does not support that version.

## Constraints

### Goals

- Let an OTLP Profiles server identify the development version before decoding a request.
- Make an unsupported version a clear, non-retryable failure instead of risking silent data loss or misinterpretation.
- Define equivalent behavior for OTLP/HTTP and OTLP/gRPC.
- Allow OTLP Profiles servers to support one or more development versions according to their implementation needs.
- Keep the mechanism temporary and limited to Profiles `v1development`.

### Non-Goals

- Introduce a global OTLP protocol version or change the evolution rules for stable signals.
- Require version discovery or negotiation between OTLP Profiles clients and servers.
- Require Collectors or backends to support previous development versions or convert between versions.
- Enforce version checks in OTLP Profiles servers released before this mechanism. Such servers do not recognize the version identifier and may continue to accept newer requests.

## Mechanism

### Metadata and Values

The metadata key is `otlp-profiles-development-version`.

For OTLP/HTTP, it is sent as a request header:

```http
OTLP-Profiles-Development-Version: 1
```

For OTLP/gRPC, it is sent as lowercase request metadata:

```text
otlp-profiles-development-version: 1
```

It applies only to the Profiles `v1development` gRPC service and HTTP endpoints serving `v1development` Profiles, including the default `/v1development/profiles` path and configured non-default paths. "Development" refers to the `v1development` lifecycle, including Alpha and Beta.

Values follow this template:

```text
<revision>
```

`revision` is a positive base-10 integer (with no leading zeros). Revision `1` is the first revision. Revisions are continuous across maturity changes within `v1development`; a maturity change alone does not change the revision.

The revision documented in [`profiles.proto`](https://github.com/open-telemetry/opentelemetry-proto/blob/8c881680d50297009b1855f688dcfb17429d63b1/opentelemetry/proto/profiles/v1development/profiles.proto) increments by one for each incompatible Profiles schema change, including changes that break wire compatibility or prevent correct interpretation of requests or responses. Compatible changes retain the current value. Between `opentelemetry-proto` releases, the revision may increase by more than one and gaps may occur. Servers match the revision against the exact set of revisions they support; ordering alone does not imply compatibility.

### Client behavior

An OTLP Profiles client selects the revision corresponding to the request format it serializes and the response format it expects. In a Collector pipeline, the OTLP exporter performs this step; source receivers that only create `pprofile.Profiles` in memory, such as the [eBPF profiler receiver](https://github.com/open-telemetry/opentelemetry-ebpf-profiler/tree/main/collector), do not set the metadata. The value applies to the entire Export request and its corresponding response. Collectors validate the incoming revision before decoding and independently identify the outgoing format; they do not need to preserve incoming metadata through the pipeline.

- A client serializing the legacy revision `1` format **MAY** omit the metadata. If it sends the metadata, it **MUST** send exactly one revision `1` value.
- A client serializing any later format **MUST** send exactly one value assigned to that format. If a client fails to do so, a version-aware server will treat the revision as `1` and may reject or decode the request incorrectly.
- If a server rejects an unsupported, malformed, or repeated revision, the client **MUST** treat the request as permanently failed. It **MUST NOT** retry sending the same telemetry data and **MUST** drop it.

### Server behavior

OTLP Profiles servers, including Collector OTLP receivers and backends, apply the following behavior.

| **Request** | **Server behavior** |
| --- | --- |
| Metadata is absent | Treat the request as revision `1` |
| Exactly one supported value is present | Decode and process the request |
| Unsupported, malformed, or repeated value | Reject the entire request before decoding |

An OTLP Profiles server **MAY** support multiple revisions but **MUST NOT** guess, downgrade, or partially process an unsupported request. Its response **MUST** be compatible with the revision identified in the request.

It returns `400 Bad Request` for OTLP/HTTP or `INVALID_ARGUMENT` for OTLP/gRPC; both are permanent, non-retryable failures under the existing [OTLP failure model](https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures).

### Compatibility and Rollout

Missing metadata identifies revision `1`. It never means "current." Existing revision `1` clients remain valid with servers supporting that revision. Supporting newer revisions does not imply support for older revisions.

Roll out the mechanism in this order:

1. Servers implement the revision check, accepting missing metadata or explicit revision `1` and rejecting unsupported values.
2. Updated Alpha clients **MAY** send revision `1`.
3. Servers add revision `2` support before revision `2` clients begin sending it. Revision `2` clients **MUST** send revision `2`.
4. A later incompatible change increments the revision to `3`.

Servers released before this mechanism may ignore the metadata and accept newer payloads. The mechanism cannot prevent this retroactively, so the rejection guarantee applies only to version-aware servers.

No discovery, preflight, or acknowledgement is required. The mechanism is retired when Profiles move from `v1development` to the `v1` service and `/v1/profiles` at Release Candidate. Clients and servers **MUST NOT** use this metadata with the `v1` Profiles service.

## Alternatives Considered

- **Payload fields or attributes.** A request field or resource or scope attribute is unavailable until the protobuf is parsed. Attributes are also repeated and semantically unrelated to the version. Neither approach enables early rejection or transport-level routing.
- **A global OTLP version or protobuf release version.** Stable signals evolve without breaking changes, making a global version unnecessarily broad. Repository SemVer covers unrelated changes and does not map directly to Profiles compatibility.
- **Versioned services and HTTP paths.** This provides the strongest isolation, including from legacy servers, but every incompatible change requires new generated packages, services, paths, and migrations. It also abandons the decision to retain `v1development` during development.
- **Negotiation, conversion, or mandatory multi-version support.** These could improve interoperability but add implementation complexity. Detecting and rejecting mismatches is the required common denominator; implementations remain free to support or convert additional versions.

## Tradeoffs

Intermediaries forwarding encoded payloads unchanged **MUST** preserve the metadata. HTTP proxies must preserve the custom header, and browser deployments may require CORS configuration. If the metadata is stripped, a newer request may appear to be legacy revision `1`.

Each incompatible change also requires coordination across clients, Collectors, and backends, with server support available before clients send the new version. These costs favor explicit failure over silent data loss or misinterpretation.
