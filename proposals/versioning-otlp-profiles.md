# Versioning OTLP Profiles

- **Author:** [Nayef Ghattas](https://github.com/Gandem)
- **Status:** Draft; protocol changes are under review in
  [opentelemetry-proto#857](https://github.com/open-telemetry/opentelemetry-proto/pull/857).
- **Originally drafted:** July 2026
- **Source:** [Original Google Doc](https://docs.google.com/document/d/1ZYf5CvxphaAXHzDxnC5PQr7Petn78-EImxjNiagwzF0/edit)

This is a working draft for discussing OTLP Profiles versioning and its design
tradeoffs. It captures the rationale for the protocol changes proposed in
[opentelemetry-proto#857](https://github.com/open-telemetry/opentelemetry-proto/pull/857),
not a separate normative specification. The mechanism below is aligned with
[revision `8c881680`](https://github.com/open-telemetry/opentelemetry-proto/blob/8c881680d50297009b1855f688dcfb17429d63b1/docs/specification.md#profiles-development-version)
of that PR.

## Summary

OTLP Profiles will continue to use `v1development` while incompatible Alpha and
Beta changes are still allowed.

We propose a temporary Profiles development version, carried in request metadata,
so servers can reject unsupported formats before deserializing requests. The
version identifies the formats of an Export request and its corresponding
response. Missing metadata means version `1`, not the current version.

The version is carried in `otlp-profiles-development-version` request metadata
for OTLP/gRPC and the `OTLP-Profiles-Development-Version` request header for
OTLP/HTTP. The mechanism is removed when Profiles moves from `v1development` to
`v1` at Release Candidate.

## Motivation

OTLP Profiles is currently Alpha and preparing for Beta. The
[Beta roadmap](https://github.com/open-telemetry/sig-profiling/issues/117)
includes at least one
[incompatible data model change](https://github.com/open-telemetry/opentelemetry-proto/pull/786),
and others may be needed before stability. This is allowed by the
[OpenTelemetry maturity definitions](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/0232-maturity-of-otel.md).

Profiles will meanwhile continue to use the same `v1development` package, gRPC
service, and HTTP endpoint until Release Candidate, following the decision in
[sig-profiling#70](https://github.com/open-telemetry/sig-profiling/issues/70)
and [opentelemetry-proto#771](https://github.com/open-telemetry/opentelemetry-proto/pull/771).

Because all development versions share the same service and endpoint, OTLP
Profiles servers cannot tell which Profiles format they are receiving before
deserializing it. Protobuf decoding can still succeed for incompatible formats
while silently dropping information. For example, the change above moves
original-payload information to fields that older servers ignore. Other changes
could instead cause decoding to fail or data to be interpreted incorrectly.

We propose adding a version identifier outside the payload to each OTLP Profiles
request. An OTLP Profiles server checks the identifier before deserializing and
rejects the request if it does not support that version.

This follows the direction discussed in
[sig-profiling#82](https://github.com/open-telemetry/sig-profiling/issues/82).

## Constraints

### Goals

- Let an OTLP Profiles server identify the development version before
  deserializing a request.
- Make an unsupported version a clear, non-retryable failure instead of risking
  silent data loss or misinterpretation.
- Identify both the Export request format and its corresponding response format.
- Define equivalent behavior for OTLP/HTTP and OTLP/gRPC.
- Allow OTLP Profiles servers to support one or more development versions
  according to their implementation needs.
- Keep the mechanism temporary and limited to Profiles `v1development`.

### Non-Goals

- Introduce a global OTLP protocol version or change the evolution rules for
  stable signals.
- Require version discovery or negotiation between OTLP Profiles clients and
  servers.
- Require Collectors or backends to support previous development versions or
  convert between versions.
- Enforce version checks in OTLP Profiles servers released before this mechanism.
  Such servers may ignore the version identifier and continue to accept newer
  requests.

## Proposed Mechanism

### Metadata and Values

For OTLP/HTTP, the version is sent as a request header:

```http
OTLP-Profiles-Development-Version: 1
```

For OTLP/gRPC, it is sent as lowercase request metadata:

```text
otlp-profiles-development-version: 1
```

The mechanism applies only to the
`opentelemetry.proto.collector.profiles.v1development.ProfilesService` service
and OTLP/HTTP endpoints serving `v1development` Profiles Export requests,
including the default `/v1development/profiles` path and configured non-default
paths. "Development" refers to the `v1development` lifecycle, including Alpha
and Beta.

The value MUST be a positive base-10 integer without leading zeros. Version `1`
is the first Profiles development version. The current version is documented in
[`profiles.proto`](https://github.com/open-telemetry/opentelemetry-proto/blob/8c881680d50297009b1855f688dcfb17429d63b1/opentelemetry/proto/profiles/v1development/profiles.proto).

For each incompatible Profiles schema change, the development version documented
in `profiles.proto` MUST be incremented by one. A schema change is incompatible
if it breaks wire compatibility or prevents correct interpretation of requests
or responses exchanged between implementations using the existing and changed
schemas. Compatible changes MUST retain the current version. A maturity change
alone does not change the version.

Between `opentelemetry-proto` releases, the development version may increase by
more than one, and gaps in the version sequence may occur. Servers match a
version against the exact set of versions they support; ordering alone does not
imply compatibility.

### Client Behavior

An OTLP Profiles client selects the version corresponding to the request format
it serializes and the response format it expects. The value applies to the
entire Export request and its corresponding response.

- A client using version `1` MAY omit the metadata.
- A client using any later version MUST send the metadata.
- When sent, the metadata MUST contain exactly one value corresponding to those
  request and response formats.

If a client using a later version omits the metadata, a version-aware server
will treat the request as version `1` and may reject or decode it incorrectly.

Rejections for malformed, repeated, or unsupported version metadata are
non-retryable. The client MUST NOT retry sending the same telemetry data and
MUST drop it. In particular, it must not resend that data without metadata or
with a different version value to try to bypass the rejection.

### Intermediary Behavior

An intermediary that forwards an Export request without decoding and re-encoding
its payload MUST preserve the Profiles development version metadata. If the
metadata is removed, a request using a later version will be treated as version
`1` and may be rejected or decoded incorrectly.

An intermediary that decodes and re-encodes Profiles data, such as the
OpenTelemetry Collector, acts as a server when receiving requests and as a
client when exporting them. It validates the incoming version before
deserialization and independently selects the version corresponding to the
request format it serializes and the response format it expects when exporting.
It does not need to preserve incoming version metadata through its pipeline.

In a Collector pipeline, the OTLP exporter selects the outgoing version. Source
receivers that only create `pprofile.Profiles` in memory, such as the
[eBPF profiler receiver](https://github.com/open-telemetry/opentelemetry-ebpf-profiler/tree/main/collector),
do not set transport metadata.

### Server Behavior

An OTLP Profiles server MAY support one or more development versions. This
includes Collector OTLP receivers and backends. It MUST handle metadata as
follows:

| Request metadata | Server behavior |
| --- | --- |
| Absent | Treat the request as version `1` |
| Exactly one well-formed value | Use the value as the request version |
| Malformed or repeated value | Reject the request before deserializing it |

If the identified version is unsupported, the server MUST reject the request
before deserializing it. It MUST NOT substitute a different version or partially
process an unsupported request.

Rejections for malformed, repeated, or unsupported metadata MUST use
`INVALID_ARGUMENT` for OTLP/gRPC or `HTTP 400 Bad Request` for OTLP/HTTP. These
are non-retryable failures under the existing
[OTLP failure model](https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures).

When returning an `ExportProfilesServiceResponse`, the server MUST use a
response format compatible with the development version identified in the
request.

### Compatibility and Rollout

Missing metadata identifies version `1`. It never means "current." Existing
clients that use version `1` therefore remain valid with servers supporting
that version. Supporting a newer version does not imply support for older
versions.

Roll out the mechanism in this order:

1. Servers implement the version check, accepting missing metadata or explicit
   version `1` and rejecting unsupported values.
2. Updated clients using version `1` MAY send version `1` explicitly.
3. Servers add support for a later version before clients begin sending that
   version. For example, version `2` clients MUST send version `2` metadata.
4. Each later incompatible schema change increments the version by one;
   compatible changes retain the current version.

Servers released before this mechanism may ignore the metadata and attempt to
process a request using an incompatible schema. The proposal cannot prevent
this retroactively, so the rejection guarantee applies only to version-aware
servers.

No discovery, preflight, or acknowledgement is required. The metadata MUST be
removed when switching from `v1development` to `v1`. The `v1` package and service
are introduced at
[Release Candidate](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/0232-maturity-of-otel.md#release-candidate),
not at the later Stable maturity level. Clients and servers MUST NOT use this
metadata with the `v1` Profiles service.

### Implementation Notes

The protocol permits servers to support one or more development versions; it
does not require a configurable version or multi-version support. A
[Collector prototype](https://github.com/open-telemetry/opentelemetry-collector/pull/16009)
uses a single compiled-in, non-configurable version for receiving and exporting.
SDK and other implementations are separate from the protocol proposal.

## Alternatives Considered

- **Payload fields or attributes.** A request field or resource or scope
  attribute requires parsing the protobuf before reading the version.
  Attributes are also repeated and semantically unrelated to the version.
  Putting the identifier in transport metadata lets a receiver reject an
  unsupported format before deserializing the Profiles request. A protobuf-field
  approach would need a version-independent envelope or other decoding strategy
  to avoid interpreting the request with an incompatible Profiles schema.
- **A global OTLP version or protobuf release version.** Stable signals evolve
  without breaking changes, making a global version unnecessarily broad.
  Repository SemVer covers unrelated changes and does not map directly to
  Profiles compatibility.
- **Versioned services and HTTP paths.** This provides the strongest isolation,
  including from legacy servers, but every incompatible change requires new
  generated packages, services, paths, and migrations. It also abandons the
  decision to retain `v1development` during development.
- **Negotiation, conversion, or mandatory multi-version support.** These could
  improve interoperability but add implementation complexity. Detecting and
  rejecting mismatches is the required common denominator; implementations
  remain free to support or convert additional versions.

## Tradeoffs

For intermediaries forwarding encoded payloads unchanged, the mechanism depends
on preserving the metadata. HTTP proxies must preserve the custom header, and
browser deployments may require CORS configuration. If the metadata is stripped,
a newer request may appear to be version `1`. Intermediaries that decode and
re-encode data instead identify the formats they use independently on each side.

Each incompatible change also requires coordination across clients, Collectors,
and backends, with server support available before clients send the new version.
These costs favor explicit failure over silent data loss or misinterpretation.
