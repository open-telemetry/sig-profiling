# OpenTelemetry Process Context - C and C++ reference implementation

This is a reference implementation of the [OpenTelemetry Process Context specification](https://github.com/open-telemetry/opentelemetry-specification/pull/4719/) for C and C++.

## What is it?

The OpenTelemetry Process Context specification defines a standard mechanism for OpenTelemetry SDKs to publish process-level resource attributes (such as service name, version, environment, etc.) in a way that can be read by out-of-process consumers, such as the [OpenTelemetry eBPF Profiler](https://github.com/open-telemetry/opentelemetry-ebpf-profiler).

This reference implementation provides:

- **A simple API** (`otel_process_ctx.h`) for publishing process context data
- **Linux-only implementation** using anonymous memory mappings with the `OTEL_CTX` signature (with a no-op fallback for other operating systems)
- **C and C++ compatibility** - the same header works for both languages

## How to Build

Use the provided build script (uses CMake):

```bash
./build.sh
```

This will create a `build/` directory containing:
- `libotel_process_ctx.so` - Shared library (C)
- `libotel_process_ctx.a` - Static library (C)
- `libotel_process_ctx_cpp.so` - Shared library (C++)
- `libotel_process_ctx_cpp.a` - Static library (C++)
- `libotel_process_ctx_noop.so` - Shared no-op library (C)
- `libotel_process_ctx_noop.a` - Static no-op library (C)
- `libotel_process_ctx_cpp_noop.so` - Shared no-op library (C++)
- `libotel_process_ctx_cpp_noop.a` - Static no-op library (C++)
- `example_ctx` - Example program
- `example_ctx_noop` - Example program (no-op variant)

(The "cpp" versions of the library are built with the C++ compiler instead of the C compiler to make sure we remain compatible on both)

### Build Requirements

- CMake 3.10 or newer
- GCC or Clang compiler with C11 and C++11 support

### Build Options

The implementation supports a no-op mode via compile-time definition:

- **`OTEL_PROCESS_CTX_NOOP`** - Compiles no-op versions of all functions (useful for non-Linux platforms or when the feature is not needed)
- **`OTEL_PROCESS_CTX_NO_READ`** - Disables read support (reduces binary size if reading is not needed)

These can be set in your own build system as needed.

## File Descriptions

### Core Implementation Files

- **`otel_process_ctx.h`** - Main header file with the complete C API. Include this in your application. Contains the `otel_process_ctx_data` struct and functions for publishing, dropping, and reading process contexts.

- **`otel_process_ctx.c`** - C implementation of the process context. Contains the core logic for creating anonymous memory mappings, encoding data as Protocol Buffers, and managing the context lifecycle.

- **`otel_process_ctx.cpp`** - C++ implementation (identical to `.c` but compiled as C++). This ensures the code works correctly when compiled with a C++ compiler.

### Example and Tools

- **`example_ctx.c`** - Example program demonstrating how to use the API. Shows publishing, reading, updating, and forking scenarios. Can run in "keep-running" mode for testing with the dump script.

- **`otel_process_ctx_dump.sh`** - Bash script to inspect a published process context from outside the process. Takes a PID as argument and dumps the context structure and payload. Useful for validation and debugging. Linux-only.

### Build Configuration

- **`CMakeLists.txt`** - CMake build configuration. Defines all library variants (C/C++, shared/static, normal/no-op) and the example program.

- **`build.sh`** - Simple build script that invokes CMake and make.

### Protocol Buffers Definitions

- **`resource.proto`** - OpenTelemetry Resource protobuf definition (extracted from OpenTelemetry protocol)
- **`common.proto`** - OpenTelemetry common types protobuf definition (AnyValue, KeyValue, etc.)

These proto files are included for reference and for use with `protoc` when decoding the payload with the dump script. The implementation includes its own minimal protobuf encoder/decoder and does not depend on the protobuf library.

## How to Use from C

### Basic Usage

1. **Include the header:**

```c
#include "otel_process_ctx.h"
```

2. **Prepare your context data:**

```c
otel_process_ctx_data data = {
    .deployment_environment_name = "production",
    .service_instance_id = "123e4567-e89b-12d3-a456-426614174000",
    .service_name = "my-service",
    .service_version = "1.2.3",
    .telemetry_sdk_language = "c",
    .telemetry_sdk_version = "0.1.0",
    .telemetry_sdk_name = "example-c",
    .resources = NULL  // Optional additional key-value pairs
};
```

3. **Publish the context:**

```c
otel_process_ctx_result result = otel_process_ctx_publish(&data);
if (!result.success) {
    fprintf(stderr, "Failed to publish context: %s\n", result.error_message);
    return 1;
}
```

4. **Drop the context when done:**

```c
if (!otel_process_ctx_drop_current()) {
    fprintf(stderr, "Failed to drop context\n");
}
```

### Adding Custom Resources

You can add custom resource attributes using the `resources` field:

```c
const char *custom_resources[] = {
    "custom.key1", "value1",
    "custom.key2", "value2",
    NULL  // Must be NULL-terminated
};

otel_process_ctx_data data = {
    // ... other fields ...
    .resources = custom_resources
};
```

### Reading the Current Context (for debugging)

```c
#ifndef OTEL_PROCESS_CTX_NO_READ
otel_process_ctx_read_result result = otel_process_ctx_read();
if (result.success) {
    printf("Service: %s\n", result.data.service_name);
    printf("Version: %s\n", result.data.service_version);

    // Don't forget to free the allocated strings
    otel_process_ctx_read_drop(&result);
} else {
    fprintf(stderr, "Failed to read: %s\n", result.error_message);
}
#endif
```

## How to Use from C++

The header is C++-compatible using `extern "C"` linkage. Usage is identical to C.

## Important API Notes

### Thread Safety

- `otel_process_ctx_publish()` and `otel_process_ctx_drop_current()` are **NOT thread-safe**. Only call these from a single thread.
- `otel_process_ctx_read()` is thread-safe for reading, but assumes no concurrent mutations.

### Fork Safety

- The memory mapping is marked with `MADV_DONTFORK`, so it does **not** propagate to child processes.
- After forking, the child process should call `otel_process_ctx_publish()` again with updated data (especially a new `service_instance_id`).
- The child process can optionally call `otel_process_ctx_drop_current()` to clean up inherited memory allocations (the payload buffer).

### String Requirements

All strings in `otel_process_ctx_data` must be:
- Non-NULL
- UTF-8 encoded
- No longer than 4096 bytes (per key/value)

Empty strings are allowed.

## Validation with otel_process_ctx_dump.sh

The `otel_process_ctx_dump.sh` script allows you to inspect a published process context from outside the process. This is useful for:
- Verifying that your application correctly publishes context
- Debugging context data
- Understanding the on-disk format

### Usage

1. **Run your application** (or the example):

```bash
./build/example_ctx --keep-running
```

This will print the PID and keep running.

2. **Dump the context** (requires root/sudo for reading `/proc/<pid>/mem`):

```bash
sudo ./otel_process_ctx_dump.sh <pid>
```

### Example Output

```
Found OTEL context for PID 267023
Start address: 756f28ce1000
00000000  4f 54 45 4c 5f 43 54 58  02 00 00 00 0b 68 55 47  |OTEL_CTX.....hUG|
00000010  70 24 7d 18 50 01 00 00  a0 82 6d 7e 6a 5f 00 00  |p$}.P.....m~j_..|
00000020
Parsed struct:
  otel_process_ctx_signature       : "OTEL_CTX"
  otel_process_ctx_version         : 2
  otel_process_ctx_published_at_ns : 1764606693650819083 (2025-12-01 16:31:33 GMT)
  otel_process_payload_size        : 336
  otel_process_payload             : 0x00005f6a7e6d82a0
Payload dump (336 bytes):
00000000  0a 25 0a 1b 64 65 70 6c  6f 79 6d 65 6e 74 2e 65  |.%..deployment.e|
00000010  6e 76 69 72 6f 6e 6d 65  6e 74 2e 6e 61 6d 65 12  |nvironment.name.|
...
```

If `protoc` is installed and the proto files are available, the script will also decode the payload:

```
Protobuf decode:
attributes {
  key: "deployment.environment.name"
  value {
    string_value: "prod"
  }
}
attributes {
  key: "service.instance.id"
  value {
    string_value: "123d8444-2c7e-46e3-89f6-6217880f7123"
  }
}
attributes {
  key: "service.name"
  value {
    string_value: "my-service"
  }
}
...
```

### Requirements for the Dump Script

- Bash shell
- Root/sudo access (to read `/proc/<pid>/mem`)
- Standard Linux utilities: `dd`, `hexdump`, `base64`, `date`
- Optional: `protoc` for Protocol Buffers decoding

## Platform Support

- **Linux only**: This implementation uses Linux-specific features (anonymous memory mappings, `prctl`, `/proc` filesystem).
- **Kernel version**: Works best on Linux 5.17+ (which supports named anonymous mappings), but includes fallback support for older kernels.
- **No-op mode**: On non-Linux platforms or when `OTEL_PROCESS_CTX_NOOP=1` is defined, all functions become no-ops.

## Example Application

The `example_ctx.c` program demonstrates the complete lifecycle:

1. **Initial publish**: Publishes a context with initial data
2. **Read and verify**: Reads back the context to verify it was published correctly
3. **Update**: Updates the context with new data
4. **Fork test**: Forks a child process that publishes its own context with a different `service_instance_id`
5. **Cleanup**: Both parent and child drop their contexts before exiting

Run the example:

```bash
./build/example_ctx
```

Or run it continuously for testing with the dump script:

```bash
./build/example_ctx --keep-running
```

## Integration Guidelines

When integrating this into an OpenTelemetry SDK or application:

1. **Call publish early**: Call `otel_process_ctx_publish()` as early as possible in your application lifecycle, once you have the service configuration available.

2. **Update on configuration changes**: If your service attributes change at runtime, call `otel_process_ctx_publish()` again with the updated data.

3. **Handle forks**: After forking, update the `service_instance_id` and call `otel_process_ctx_publish()` in the child process.

4. **Clean up on exit**: Call `otel_process_ctx_drop_current()` during shutdown (optional but recommended for clean exit).

5. **Check result codes**: Always check the `success` field of the result and log errors appropriately.

6. **Consider no-op builds**: For non-Linux platforms, use the no-op variant or define `OTEL_PROCESS_CTX_NOOP=1`.

## Specification Compliance

This implementation follows the [OpenTelemetry Process Context specification](https://github.com/open-telemetry/opentelemetry-specification/pull/4719/). Key aspects:

- Uses the `OTEL_CTX` signature for discoverability
- Stores data in version 2 format (packed struct + protobuf payload)
- Encodes resource attributes using OpenTelemetry Protocol Buffers Resource format
- Supports both standard semantic conventions and custom resource attributes
- Provides proper memory ordering guarantees for concurrent readers

## License

Unless explicitly stated otherwise, all files in this repository are licensed under the Apache License, Version 2.0. This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.
