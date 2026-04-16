# Changelog

All notable changes to this project are documented in this file.

## [0.2.0] - 2026-04-16

Second release for the EntryPoint event ingestion pipeline, following the v0.1.0 release format.

### ✨ Features
- **Repository naming consistency** — updated project references from `entry_point` to `entry-point` in user-facing documentation and links.

### 🧱 Architecture
- **No runtime architecture changes** — pipeline components and data flow remain aligned with v0.1.0.

### 📚 Scope
- **Release/documentation refresh** — this release focuses on release hygiene and repository reference consistency.

**Full changelog**: https://github.com/cr4n/entry-point/compare/v0.1.0...v0.2.0

## [0.1.0] - 2026-04-16

Initial release for the EntryPoint event ingestion pipeline and dashboard baseline.

### ✨ Features
- **Near-real-time EntryPoint ingestion** — listens for `UserOperationEvent` logs from Alchemy and streams them into RabbitMQ for downstream processing.
- **PostgreSQL persistence** — stores raw user operation event data in `pipeline.raw_user_operations` and loads bundler reference data at initialization.
- **Grafana dashboard baseline** — ships a containerized dashboard for monitoring bundler activity and pipeline outputs.

### 🧱 Architecture
- **Containerized local stack** — Docker Compose wires together the listener, consumer, PostgreSQL, RabbitMQ, and Grafana services.
- **Alchemy → RabbitMQ → Postgres flow** — clear module separation between event collection, queue-based transport, and database writes.

### 📚 Scope
- **Streaming-only behavior** — the pipeline processes newly observed events and does not backfill historical chain data.

**Full changelog**: https://github.com/cr4n/entry-point/commits/v0.1.0
