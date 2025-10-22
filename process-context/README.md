# OpenTelemetry Process Context (Under development)

This folder currently contains the development protobuf definitions for the specification proposed in https://docs.google.com/document/d/1-4jo29vWBZZ0nKKAOG13uAQjRcARwmRc4P313LTbPOE/edit?tab=t.0 .

The `OtelProcessCtx` message is used to allow in-process libraries (such as the OTel SDKs running inside an application) to share information with outside readers (such as the OpenTelemetry eBPF Profiler).
