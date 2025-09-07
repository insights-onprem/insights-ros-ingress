# Insights ROS Ingress

A specialized ingress service for processing HCCM (Hybrid Cloud Cost Management) uploads and extracting Resource Optimization Service (ROS) data for on-premise deployments.

## Overview

This service combines the upload handling capabilities of `insights-ingress-go` with the ROS data extraction logic from `koku`, eliminating the need for koku as an intermediary. It processes OpenShift cluster payloads, extracts ROS CSV files, uploads them to MinIO storage, and notifies the ROS processor via Kafka.

## Architecture

```
HCCM Upload → insights-ros-ingress → MinIO (ROS bucket) → Kafka (ROS events) → ROS Processor
```

## Features

- **HCCM Upload Processing**: Handles `application/vnd.redhat.hccm.upload` content-type
- **Payload Extraction**: Extracts and validates tar.gz payloads with manifest.json
- **ROS File Processing**: Identifies and processes resource optimization CSV files
- **MinIO Integration**: S3-compatible storage for on-premise deployments
- **Kafka Integration**: Sends events to `hccm.ros.events` topic
- **OpenShift Deployment**: Helm chart with native OpenShift support
- **Cloud-Native**: Designed for Kubernetes without Clowder dependency

## Quick Start

### Using Podman Compose

```bash
# Start dependencies
podman-compose -f scripts/docker-compose.yml up -d

# Build and run
make build
make run
```

### Using Helm on OpenShift

```bash
# Deploy with Helm
helm install insights-ros-ingress ./deployments/helm/insights-ros-ingress
```

## Configuration

The service uses Kubernetes ConfigMaps and Secrets for configuration, mimicking Clowder behavior without the dependency:

- **ConfigMaps**: Application configuration
- **Secrets**: MinIO and Kafka credentials
- **Environment Variables**: Service discovery endpoints

## Development

### Prerequisites

- Go 1.21+
- Podman 4.0+
- Make

### Building

```bash
# Build binary
make build

# Build container image
make image

# Run tests
make test

# Run linting
make lint
```

## Project Structure

```
├── cmd/insights-ros-ingress/    # Main application entry point
├── internal/                   # Private application code
│   ├── config/                 # Configuration management
│   ├── upload/                 # HTTP upload handlers
│   ├── storage/                # MinIO storage client
│   ├── messaging/              # Kafka producer
│   ├── logger/                 # Logging utilities
│   └── health/                 # Health check endpoints
├── deployments/helm/           # Helm charts for deployment
├── docs/                       # Documentation
└── scripts/                    # Development scripts
```

## API Endpoints

- `POST /api/ingress/v1/upload` - Upload HCCM payload
- `GET /health` - Health check
- `GET /ready` - Readiness probe
- `GET /metrics` - Prometheus metrics

## Testing

### Unit Tests
```bash
make test
```

### Integration Testing

Complete end-to-end integration test with docker-compose services:

```bash
# Full integration test (recommended)
make test-integration

# Quick test with existing services
make dev-env-up
make test-integration-quick

# Manual testing
make dev-env-up
make run-test  # In one terminal
# Test upload API in another terminal
```

The integration test validates:
- Docker-compose services (MinIO, Kafka, Zookeeper)
- ROS data upload and processing
- MinIO file storage and metadata
- Kafka message publishing

For detailed testing instructions, see [docs/testing.md](docs/testing.md).

## Contributing

Please follow the guidelines in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0