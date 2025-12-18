#!/usr/bin/env bash
set -euo pipefail

# Unless explicitly stated otherwise all files in this repository are licensed under the Apache License (Version 2.0).
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

# otel_process_ctx_dump.sh
# Usage: ./otel_process_ctx_dump.sh <pid>
#
# Reads the OTEL process context mapping for a given PID, parses the struct,
# and dumps the payload as well.

check_otel_signature() {
  # Check that the first 8 bytes are "OTEL_CTX"
  [ "$(printf '%s' "$1" | base64 -d | dd bs=1 count=8 status=none)" = "OTEL_CTX" ]
}

if [ "$(uname -s)" != "Linux" ]; then
  echo "Error: this script only supports Linux." >&2
  exit 1
fi

pid="${1:-}"
if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
  echo "Usage: $0 <pid>" >&2
  exit 1
fi

# Find the mapping by name
if ! line="$(grep -F -m 1 -e '[anon_shmem:OTEL_CTX]' -e '/memfd:OTEL_CTX' "/proc/$pid/maps")"; then
  echo "No OTEL_CTX context found." >&2
  exit 1
fi

start_addr="${line%%-*}"

echo "Found OTEL context for PID $pid"
echo "Start address: $start_addr"

# Read struct otel_process_ctx_mapping, encode as base64 so we can safely store it in a shell variable.
# (Bash variables cannot hold NUL bytes, so raw binary causes issues)
data_b64="$(dd if="/proc/$pid/mem" bs=1 count=32 skip=$((16#$start_addr)) status=none | base64 -w0)"

# Pretty-print otel_process_ctx_mapping
printf '%s' "$data_b64" | base64 -d | hexdump -C

# Check that the first 8 bytes are "OTEL_CTX"
check_otel_signature "$data_b64"

# Extract fields from otel_process_ctx_mapping
signature="$(
  printf '%s' "$data_b64" | base64 -d | dd bs=1 count=8 status=none
)"
version="$(
  printf '%s' "$data_b64" | base64 -d | dd bs=1 skip=8 count=4 status=none | od -An -t u4 | tr -d ' '
)"
payload_size="$(
  printf '%s' "$data_b64" | base64 -d | dd bs=1 skip=12 count=4 status=none | od -An -t u4 | tr -d ' '
)"
published_at_ns="$(
  printf '%s' "$data_b64" | base64 -d | dd bs=1 skip=16 count=8 status=none | od -An -t u8 | tr -d ' '
)"
payload_ptr_hex="$(
  printf '%s' "$data_b64" | base64 -d | dd bs=1 skip=24 count=8 status=none | od -An -t x8 | tr -d ' '
)"

echo "Parsed struct:"
echo "  otel_process_ctx_signature       : \"$signature\""
echo "  otel_process_ctx_version         : $version"
# Convert nanoseconds to seconds for date command
published_at_s=$((published_at_ns / 1000000000))
published_at_pretty="$(date -d "@$published_at_s" '+%Y-%m-%d %H:%M:%S %Z')"
echo "  otel_process_payload_size        : $payload_size"
echo "  otel_process_ctx_published_at_ns : $published_at_ns ($published_at_pretty)"
echo "  otel_process_payload             : 0x$payload_ptr_hex"

echo "Payload dump ($payload_size bytes):"
dd if="/proc/$pid/mem" bs=1 count="$payload_size" skip=$((16#$payload_ptr_hex)) status=none | hexdump -C

if command -v protoc >/dev/null 2>&1; then
  echo "Protobuf decode:"
  dd if="/proc/$pid/mem" bs=1 count="$payload_size" skip=$((16#$payload_ptr_hex)) status=none | protoc --decode=opentelemetry.proto.resource.v1.Resource resource.proto common.proto
else
  echo
  echo "protoc not available - skipping protobuf decode"
fi
