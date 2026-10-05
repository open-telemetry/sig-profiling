# Versioning OTLP Profiles

- **Author:** [Nayef Ghattas](https://github.com/Gandem)
- **Status:** Accepted; specification change approved in
  [opentelemetry-proto#857](https://github.com/open-telemetry/opentelemetry-proto/pull/857).
- **Originally documented:** July 2026
- **Source:** [Original Google Doc](https://docs.google.com/document/d/1ZYf5CvxphaAXHzDxnC5PQr7Petn78-EImxjNiagwzF0/edit)

This document records the context and rationale behind the decision to identify
OTLP Profiles development formats using request metadata. It preserves the
motivation, alternatives, rollout considerations, and tradeoffs beyond the
specification itself; it is not a separate normative specification or an open
proposal.

The protocol requirements are defined by the approved
[specification change](https://github.com/open-telemetry/opentelemetry-proto/blob/8c881680d50297009b1855f688dcfb17429d63b1/docs/specification.md#profiles-development-version)
in [opentelemetry-proto#857](https://github.com/open-telemetry/opentelemetry-proto/pull/857).
The linked revision records the specification text on which this decision record
is based.

## Summary

OTLP Profiles retains `v1development` while incompatible Alpha and Beta changes
are still allowed. The decision is to use a temporary Profiles development
version, carried in request metadata, so servers can reject unsupported formats
before deserializing requests.

The version identifies the formats of an Export request and its corresponding
response. It is carried in `otlp-profiles-development-version` request metadata
for OTLP/gRPC and the `OTLP-Profiles-Development-Version` request header for
OTLP/HTTP. Missing metadata means version `1`, not the current version.

The mechanism is removed when Profiles moves from `v1development` to `v1` at
Release Candidate. It does not introduce versioning for stable OTLP signals.

## Context and Motivation

At the time of this decision, OTLP Profiles was Alpha and preparing for Beta.
The [Beta roadmap](https://github.com/open-telemetry/sig-profiling/issues/117)
included at least one
[incompatible data model change](https://github.com/open-telemetry/opentelemetry-proto/pull/786),
and others could be needed before stability. This is allowed by the
[OpenTelemetry maturity definitions](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/0232-maturity-of-otel.md).

Profiles retains the same `v1development` package, gRPC service, and HTTP endpoint
until Release Candidate, following the decision in
[sig-profiling#70](https://github.com/open-telemetry/sig-profiling/issues/70)
and [opentelemetry-proto#771](https://github.com/open-telemetry/opentelemetry-proto/pull/771).

Because all development versions share the same service and endpoint, OTLP
Profiles servers cannot tell which Profiles format they are receiving before
deserializing it. Protobuf decoding can still succeed for incompatible formats
while silently dropping information. For example, the change above moves
original-payload information to fields that older servers ignore. Other changes
could instead cause decoding to fail or data to be interpreted incorrectly.

Adding a version identifier outside the payload lets an OTLP Profiles server
check the identifier before deserializing and reject the request if it does not
support that version. This follows the direction discussed in
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

## Decision and Rationale

### Identify the Format Before Deserialization

The identifier is transport metadata rather than part of the Profiles payload.
This allows a receiver to reject an unsupported format before deserializing the
request with an incompatible Profiles schema. Equivalent metadata is available
in both OTLP/gRPC and OTLP/HTTP:

```text
otlp-profiles-development-version: 1
```

```http
OTLP-Profiles-Development-Version: 1
```

The mechanism covers the Profiles `v1development` gRPC service and HTTP endpoints
serving `v1development` Profiles Export requests, including configured non-default
paths. Its scope is the development format, not a particular HTTP path or a global
OTLP protocol version.

### Version Incompatible Formats, Not Releases or Maturity Levels

The development version is a positive base-10 integer without leading zeros.
Version `1` is the first development version; the version is documented alongside
the Profiles schema in
[`profiles.proto`](https://github.com/open-telemetry/opentelemetry-proto/blob/8c881680d50297009b1855f688dcfb17429d63b1/opentelemetry/proto/profiles/v1development/profiles.proto).

Each incompatible Profiles schema change increments the version by one.
Incompatibility includes broken wire compatibility and changes that prevent
correct interpretation of requests or responses. Compatible changes retain the
version. A maturity transition alone does not change it.

Tying increments to incompatible changes, rather than repository releases,
keeps the compatibility decision with the people making the schema change.
Multiple increments may occur between releases, and gaps may occur in the
released version sequence. Ordering does not imply compatibility: servers
support an explicit set of versions.

### Preserve Existing Clients Without Treating Missing Metadata as Current

Missing metadata identifies version `1`. Clients using that version can omit
it, while clients using later versions send exactly one value corresponding to
the request format they serialize and the response format they expect. This
preserves compatibility with existing version `1` clients without making an
unlabeled request change meaning as the schema evolves.

The version covers the entire Export request and its corresponding response.
A server returns a response compatible with the version identified in the
request, so incompatibilities in the response schema are covered as well.

### Fail Explicitly Rather Than Guess

Version-aware servers reject malformed, repeated, or unsupported version
metadata before deserializing the request. They do not substitute a different
version or partially process an unsupported request. Rejections use
`INVALID_ARGUMENT` for OTLP/gRPC or `HTTP 400 Bad Request` for OTLP/HTTP.

These are permanent, non-retryable failures under the existing
[OTLP failure model](https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures).
Clients drop the rejected telemetry rather than retrying the same data with
missing or different metadata. The identifier detects a mismatch; it does not
provide negotiation or conversion.

### Treat Receiving and Exporting as Separate Compatibility Boundaries

An intermediary forwarding an encoded request unchanged preserves its metadata.
Otherwise a later-version request may appear to be version `1` and be rejected
or decoded incorrectly.

An intermediary that decodes and re-encodes data, such as the OpenTelemetry
Collector, validates the incoming version before deserialization and independently
identifies the request format it serializes and response format it expects when
exporting. It does not need to preserve incoming metadata through its pipeline.

In a Collector pipeline, the OTLP exporter selects the outgoing version. Source
receivers that only create `pprofile.Profiles` in memory, such as the
[eBPF profiler receiver](https://github.com/open-telemetry/opentelemetry-ebpf-profiler/tree/main/collector),
do not set transport metadata.

### Keep the Mechanism Limited to Development

The identifier is removed when switching from `v1development` to `v1` and is not
used with the `v1` Profiles service. The `v1` package and service are introduced
at [Release Candidate](https://github.com/open-telemetry/opentelemetry-specification/blob/main/oteps/0232-maturity-of-otel.md#release-candidate),
not at the later Stable maturity level. The mechanism exists to manage
incompatible development formats, not to replace stable OTLP evolution rules.

## Rollout and Implementation Context

The rollout sequence is:

1. Servers implement the version check, accepting missing metadata or explicit
   version `1` and rejecting unsupported values.
2. Updated clients using version `1` can send version `1` explicitly.
3. Servers add support for a later version before clients begin sending that
   version. For example, version `2` clients send version `2` metadata.
4. Each later incompatible schema change increments the version by one;
   compatible changes retain the current version.

Supporting a newer version does not imply support for older versions. Servers
released before this mechanism may ignore the metadata and attempt to process
an incompatible payload. The decision cannot prevent this retroactively, so the
rejection guarantee applies only to version-aware servers. No discovery,
preflight, or acknowledgement is required.

The protocol permits one or more supported versions; it does not require a
configurable version or multi-version support. A
[Collector prototype](https://github.com/open-telemetry/opentelemetry-collector/pull/16009)
uses a single compiled-in, non-configurable version for receiving and exporting.
Implementation work is separate from the specification change.

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
  rejecting mismatches is the common denominator; implementations remain free
  to support or convert additional versions.

## Tradeoffs

For intermediaries forwarding encoded payloads unchanged, the mechanism depends
on preserving the metadata. HTTP proxies must preserve the custom header, and
browser deployments may require CORS configuration. If the metadata is stripped,
a newer request may appear to be version `1`. Intermediaries that decode and
re-encode data instead identify the formats they use independently on each side.

Each incompatible change also requires coordination across clients, Collectors,
and backends, with server support available before clients send the new version.
These costs favor explicit failure over silent data loss or misinterpretation.
