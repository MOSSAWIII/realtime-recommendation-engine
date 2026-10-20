# Real-Time Recommendation Engine

**Event-driven recommendation system · Scala · RabbitMQ · PostgreSQL**

A multi-component Scala project for turning user interactions into recommendation requests and results through an event-driven pipeline. This repository contains application logic, SQL schemas, infrastructure configuration, and unit/integration-oriented test code.

## Why this project exists

Serving relevant recommendations as interactions arrive requires more than a ranking formula. The system must ingest events, transform them into a usable representation, access historical state, compute candidate scores, and communicate results reliably.

## Components

- **Transformer** — processes and enriches incoming user events and generates recommendation requests.
- **DataManager** — manages access to application data and historical state.
- **Recommender** — computes personalized recommendations from available interaction and relational signals.
- **RabbitMQ** — routes user actions, processed events, and recommendation-related messages.
- **PostgreSQL** — persists users, events, participation and relationship data.

These components are visible in `src/main/scala/`; further architectural detail is available in [the technical architecture](docs/TechnicalArchitecture.md). Diagrams in `docs/` may describe earlier designs; consult the code and the technical architecture before treating a diagram as authoritative.

## Stack

Scala 2.13, sbt, RabbitMQ, PostgreSQL, HikariCP, Circe, Docker Compose, ScalaTest and Testcontainers.

## Explore the implementation

- [Application components](src/main/scala/application/core)
- [Domain models](src/main/scala/domain/models)
- [Architecture and design notes](docs/TechnicalArchitecture.md)
- [Testing guide](docs/Tests.md)
- [Test source tree](src/test/scala)
- [Container configuration](docker-compose.yml)

## Local development

Prerequisites: JDK, sbt and Docker with Compose.

```bash
sbt compile
sbt test
docker compose up --build
```

The commands are starting points for an appropriately configured local environment, **not** a claim that the complete stack was freshly executed for this documentation update. The Compose configuration contains **development-only example credentials**; do not reuse them for deployment or expose the services to untrusted networks.

## Project status and limitations

This repository is shared as an engineering and architectural work sample. It has not been independently revalidated as a production-ready service as part of this presentation update. Claims about throughput, latency, recommendation quality, and deployment readiness require their own reproducible benchmarks.

## Scope and provenance

This public repository describes a recommendation-system engineering project developed during my data consulting work. Public visibility does not imply authorization to disclose private client data or proprietary systems; the published content should be reviewed for any third-party intellectual-property constraints before further reuse.
