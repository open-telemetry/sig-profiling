# OTLP Profiling Benchmark Setup

This setup captures real-world OTLP profiling data to measure the impact of protocol changes on in-memory and wire size.

## Overview

We run two workloads on a single-node minikube cluster:

- **[OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo)** (Astronomy Shop): A fictional e-commerce application composed of ~15 microservices written in Go, Java, Python, .NET, Node.js, Rust, PHP, and more. Generates diverse profiling data across different runtimes and frameworks.
- **[NetBox](https://github.com/netbox-community/netbox)**: A network infrastructure management platform (IPAM/DCIM) built with Django. Runs as a gunicorn WSGI application that spawns multiple worker processes—useful for evaluating profiler behavior with forking Python processes.

A custom OpenTelemetry Collector with the [eBPF profiler](https://github.com/open-telemetry/opentelemetry-ebpf-profiler) captures profiles from all processes on the node and writes them to disk.

## Prerequisites

- Ubuntu 24.04 VM (tested on AWS c6a.8xlarge: 32 vCPUs, 64 GiB)
- Sudo access

## Setup

### 1. Install dependencies

```bash
./install.sh
```

This installs Docker, minikube, kubectl, helm, and configures the system for Kubernetes.

After installation, either log out and back in, or run `newgrp docker` to activate Docker group membership.

### 2. Start the cluster

```bash
minikube start --driver=none
```

### 3. Deploy workloads

**OpenTelemetry Demo:**

```bash
kubectl create namespace otel-demo
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
helm install otel-demo open-telemetry/opentelemetry-demo -n otel-demo --values k8s/opentelemetry-demo/values.yaml
```

**NetBox:**

```bash
kubectl create namespace netbox-bench
helm install netbox-bench oci://ghcr.io/netbox-community/netbox-chart/netbox -n netbox-bench
```

NetBox takes ~10 minutes to start (database migrations). Wait for all pods to be ready:

```bash
kubectl get pods -n netbox-bench -w
```

Then start the load generator:

```bash
kubectl apply -f k8s/netbox/load-generator.yaml
```

### 4. Deploy the collector

Build a custom collector pinned to a specific commit of the eBPF profiler:

```bash
./build-custom-collector.sh
```

Deploy it:

```bash
kubectl create namespace otel
kubectl apply -f k8s/opentelemetry-collector/
```

## Output

Profiles are written to `/var/lib/otel-profiles/profiles.proto` in the [file exporter format](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/fileexporter#file-format).

## Notes

- The collector runs as a DaemonSet with privileged access (required for eBPF).
- We use a custom Kubernetes manifest because the collector Helm charts don't yet support the profiling distribution.
- The `builder-manifest.yaml` pins the eBPF profiler to a [specific commit](https://github.com/open-telemetry/opentelemetry-ebpf-profiler/tree/fd60ef3f4a81577e4269bf821b11b38b81fadb52) for reproducibility. It includes [#889](https://github.com/open-telemetry/opentelemetry-ebpf-profiler/pull/889) which makes collector logs more configurable.
