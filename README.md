# Outbox Event Relay

## Overview

The **Outbox Event Relay** is a Node.js service implementing the transactional outbox pattern to solve the dual-write problem in distributed systems. When an application needs to update a database and publish an event to a message broker, these two operations cannot be made atomic across different systems. If the database transaction succeeds but the message publish fails (or vice versa), the system enters an inconsistent state where data and events are out of sync.

This service guarantees that business events are published to Redis Streams exactly once, even when failures occur between database commits and message broker operations. The pattern works by recording events in a PostgreSQL outbox table within the same transaction as the business data. A separate relay worker continuously reads unpublished events from the outbox and publishes them to Redis Streams, marking them as sent only after successful delivery. This design ensures that if a crash happens at any point, events are never lost and never duplicated.

Built with TypeScript, the service consists of an HTTP API that accepts business events and stores them transactionally, and a background worker that polls the outbox table and relays events to Redis Streams. The worker uses PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED` to enable horizontal scaling without duplicate processing. The system also includes PostgreSQL LISTEN/NOTIFY for instant event processing, reducing latency compared to polling alone. Cleanup workers automatically remove old sent events to prevent unbounded table growth.

The relay is designed to sit between transactional application services and downstream event consumers. Any service can submit a business event, while analytics, notification, reporting, and other consumers process the resulting Redis Stream independently.

**Tech Stack:** Node.js, TypeScript, Express, PostgreSQL, Prisma, Redis Streams, Zod, Pino

---

## System Architecture

![System Architecture](images/architecture.png)

The architecture illustrates one possible integration with an upstream application and several independent consumers. The relay itself is domain-agnostic and can support payments, orders, notifications, analytics, or other event-driven workflows.

---

## Event Processing Flow

![Event Processing Flow](images/flow.png)

The flow begins when a client posts an event to the HTTP API. The event service validates the payload using Zod schemas, then creates both a business event record (for audit history) and an outbox event record (for relay processing) within a single atomic transaction. PostgreSQL NOTIFY triggers immediately alert the relay worker, which acquires a lock on pending events using `SELECT FOR UPDATE SKIP LOCKED`, publishes them to Redis Streams, and updates their status. Failed publishes are retried with exponential backoff until max attempts are reached.

---

## Reliability & Guarantees

![Reliability & Guarantees](images/reliability.png)

The reliability diagram illustrates the five core guarantees: atomic commitment ensures events and outbox entries are created together or not at all, preventing orphaned records. No events are lost because they are persisted in the database before attempting any external publish. Duplicates are prevented through unique constraints on aggregate ID and event type, combined with row-level locking during relay. Events are processed in creation order, maintaining causal consistency. At-least-once delivery is guaranteed through persistent retry logic with exponential backoff.

---

## File Structure & Explanation

### Core Application Files

#### `src/server.ts`

The main entry point for the Express HTTP server. Sets up middleware including Pino HTTP logger for structured request logging and JSON body parsing. Registers all API routes (`/events`, `/health`, `/metrics`, `/outbox`) and implements error handling with a 404 handler for undefined routes and a global error handler for unexpected exceptions. The server includes graceful shutdown logic that responds to SIGTERM and SIGINT signals, allowing in-flight requests to complete before closing connections. It handles unhandled promise rejections and uncaught exceptions to prevent silent failures that could leave the system in an unknown state.

#### `src/worker/relay.ts`

The core outbox relay worker that polls the outbox table and publishes events to Redis Streams. This runs as a separate process from the HTTP server, enabling independent scaling based on event throughput. The worker implements a dual-trigger mechanism: it polls on a configurable interval (default 1 second) and also listens for PostgreSQL NOTIFY events for immediate processing. The `processOutboxBatch` method uses raw SQL with `SELECT FOR UPDATE SKIP LOCKED` to fetch pending events, preventing multiple workers from processing the same event. For each event, it calls the Redis publisher, updates the event status to sent or failed, increments attempt counters, and records errors. The worker tracks metrics including queue size, processing duration, and success/failure counts for observability.

#### `src/worker/cleanup.ts`

Manages automatic cleanup of old sent events from the outbox table to prevent unbounded growth. Runs on a 24-hour interval and deletes events with status sent that are older than the configured retention period (default 7 days). This cleanup is safe because sent events have already been successfully delivered to Redis Streams and are no longer needed for relay purposes. The business event table retains the full event history for auditing, while the outbox table only needs recent events for operational purposes. Failed events are never cleaned up automatically, requiring manual intervention to investigate and resolve the underlying issues.

#### `src/services/eventService.ts`

Contains the business logic for creating events with the transactional outbox pattern. The `createBusinessEvent` method validates the event payload using Zod, then wraps two database inserts in a Prisma transaction: one for the business event table (permanent audit log) and one for the outbox event table (temporary relay queue). After both inserts, it executes a PostgreSQL NOTIFY command to immediately trigger the relay worker, reducing latency compared to polling alone. If a duplicate event arrives (Prisma error P2002 from the unique constraint on aggregate ID and event type), the service queries for the existing outbox event and returns its details, allowing the caller to identify duplicates without creating new records.

#### `src/queue/publisher.ts`

Handles the actual publishing of events to Redis Streams using the ioredis XADD command. Implements a three-attempt retry loop with exponential backoff (2 seconds, 4 seconds) to handle transient Redis connection issues. Each publish includes the event ID, event type, aggregate ID, payload (stringified JSON), and timestamp as stream fields. The publisher returns the Redis-generated message ID on success, which could be stored for additional tracking if needed. If all retry attempts fail, it throws an error that the relay worker catches, increments the attempt counter, and either retries later or marks the event as failed based on the max retry configuration.

### Route Handlers

#### `src/routes/events.ts`

Provides the HTTP endpoint for creating business events. The POST handler validates the request body against a Zod schema requiring event type, aggregate ID, and payload fields. Valid events are passed to the event service for transactional creation. The endpoint returns 201 Created with both the business event ID (for audit trail) and outbox event ID (for tracking relay status). Invalid payloads return 400 Bad Request with detailed validation errors. Unexpected errors return 500 Internal Server Error with a generic message to avoid leaking implementation details to clients.

#### `src/routes/outbox.ts`

Exposes three monitoring endpoints for outbox state inspection. GET `/outbox/status` returns aggregate counts of pending, sent, and failed events, plus the age of the oldest pending event for identifying processing lag. GET `/outbox/failed` returns a paginated list of failed events with error messages, event types, and attempt counts for operational troubleshooting. GET `/outbox/pending` returns pending events ordered by creation time, useful for diagnosing backlog issues. All endpoints support optional event type filtering and standard pagination parameters (limit, offset) with maximum limits to prevent memory exhaustion.

#### `src/routes/health.ts`

Implements a health check endpoint that tests connectivity to both PostgreSQL and Redis dependencies. Executes actual queries (SELECT 1 for PostgreSQL, PING for Redis) rather than checking connection status, ensuring the health check reflects real readiness to serve traffic. Measures and returns latency for each dependency, enabling performance monitoring. Returns 200 OK if all dependencies are healthy, or 503 Service Unavailable if any dependency fails. This endpoint supports Kubernetes liveness and readiness probes, load balancer health checks, and manual debugging.

#### `src/routes/metrics.ts`

Exposes Prometheus-format metrics at GET `/metrics` for observability integration. Includes Node.js default metrics (memory usage, CPU, event loop lag) plus custom application metrics: `outbox_events_total` counter by status, `outbox_relay_duration_seconds` histogram with buckets for processing time, `outbox_queue_size` gauge for current pending count, `redis_publish_errors_total` counter by error type, and `outbox_cleanup_events_total` counter for deleted events. These metrics enable dashboards showing event throughput, processing latency, queue depth trends, error rates, and capacity planning.

### Database

#### `prisma/schema.prisma`

Defines two PostgreSQL models: **BusinessEvent** stores the permanent event history with event type, aggregate ID, and JSON payload. This table grows indefinitely as the audit log and uses indexes on aggregate ID and event type for efficient querying. **OutboxEvent** stores events pending relay with the same core fields plus status (pending, sent, failed), attempt counter, error message, and sent timestamp. The unique constraint on aggregate ID and event type enforces idempotency at the database level, preventing duplicate event creation. The composite index on status and created timestamp optimizes the worker query that fetches pending events in creation order.

#### `src/db/client.ts` and `src/db/index.ts`

Simple re-exports of the Prisma client instance for centralized database access. Prisma manages connection pooling automatically, with connections shared across the application. In development mode, the client uses global singleton pattern to prevent hot reload from exhausting connection pools.

#### `src/db/notify.ts`

Manages a dedicated PostgreSQL connection for LISTEN/NOTIFY functionality separate from the Prisma connection pool. Creates a pg client that connects and issues `LISTEN outbox_event`, then registers a notification handler that triggers the relay worker's batch processing. Implements automatic reconnection with a 5-second delay when the connection drops, ensuring the NOTIFY mechanism remains active even through transient database issues. This push-based notification reduces average latency compared to polling alone, enabling near-instant event relay when the system is under load.

### Utilities

#### `src/utils/config.ts`

Manages environment variable configuration with type-safe validation using Zod schemas. Defines schemas for database URL, Redis URL, server port and environment, worker settings (poll interval, batch size, max retries), stream name, log level, and cleanup retention days. The validation happens at startup using `safeParse`, and any missing or invalid configuration causes the application to exit immediately with clear error messages before accepting any traffic. This fail-fast approach prevents runtime errors from misconfiguration. Default values are provided for optional settings like port (3001) and poll interval (1000ms).

#### `src/utils/logger.ts`

Sets up the Pino structured logger with environment-specific configuration. In development, uses `pino-pretty` for human-readable colored output with timestamps. In production, outputs newline-delimited JSON for log aggregation systems like ELK, Datadog, or CloudWatch. Structured logging includes contextual fields (event ID, aggregate ID, status) in each log entry, enabling efficient searching and correlation across distributed systems.

### Type Definitions

#### `src/types/*.ts`

TypeScript type definitions organized by domain: `config.types.ts` for application configuration, `event.types.ts` for event creation parameters and results, `worker.types.ts` for relay job and result types. The `index.ts` barrel file re-exports all types for convenient importing. These types provide compile-time safety and IDE autocomplete, reducing bugs from typos or incorrect data structures.

---

## Design Decisions & Trade-offs

### Why Implement the Transactional Outbox Pattern?

The fundamental problem in distributed systems is the dual-write dilemma: you cannot atomically update a database and publish a message to a broker in a single transaction. If you update the database first and then publish fails, the database has changed but no event was sent. If you publish first and the database update fails, consumers receive an event that never actually happened. **The outbox pattern solves this by making the event part of the database transaction itself.** By writing events to an outbox table in the same transaction as the business data, both succeed or fail together. A separate relay worker handles publishing, retrying until successful. The trade-off is increased complexity (an additional table and worker process) and eventual consistency (events are not published instantly), but these costs are acceptable for the reliability gains in systems where lost or duplicate events cause business problems.

### Why Use SELECT FOR UPDATE SKIP LOCKED?

When running multiple relay workers for horizontal scaling, standard row locking would cause workers to wait for each other, reducing throughput. **SELECT FOR UPDATE SKIP LOCKED** allows multiple workers to process different events concurrently without blocking. Each worker locks only the rows it will process, and other workers skip locked rows, moving on to other available events. This enables linear scaling of relay workers based on event volume. The trade-off is slightly more complex query logic and the requirement that events can be processed in any order (within the constraint of creation time ordering). For event streaming where consumers handle ordering through stream offsets, this trade-off is acceptable.

### Why Combine Polling and PostgreSQL NOTIFY?

Polling alone provides reliability (the worker will eventually process events) but at the cost of latency (events wait until the next poll cycle). **NOTIFY provides instant triggering** when new events arrive, reducing latency to milliseconds in the common case. However, NOTIFY alone is unreliable because notifications can be lost during connection drops. **Combining both mechanisms** provides the best of both: instant processing when NOTIFY works, and guaranteed processing through polling even when notifications are missed. The implementation uses a 1-second poll interval as backup, ensuring events are never delayed more than one second even if NOTIFY fails completely. This hybrid approach adds complexity but delivers both low latency and reliability.

### Why Separate Business Event and Outbox Event Tables?

The business event table serves as a permanent audit log of all events that occurred in the system, while the outbox table is a temporary work queue for relay processing. **Separating these concerns** allows different retention policies: business events are kept forever (or for regulatory compliance periods), while outbox events are cleaned up after successful delivery. The outbox table can be optimized for relay queries (indexes on status and creation time) without impacting audit queries (indexes on aggregate ID and event type). The trade-off is additional storage and slightly more complex transaction code (two inserts instead of one), but this separation of concerns improves both operational flexibility and query performance.

### Why Use Redis Streams Instead of Other Brokers?

Redis Streams provide several advantages for event distribution: consumer groups enable multiple consumers to share work without duplicates, each consumer can track its own offset independently, the stream retains messages even after consumption (unlike traditional pub/sub), and Redis is widely deployed and understood. **The choice prioritizes simplicity and operational familiarity** over the advanced features of systems like Kafka or RabbitMQ. For many applications, Redis Streams provide sufficient durability, ordering, and scalability without the operational complexity of running a separate message broker cluster. The trade-off is less durability than Kafka (Redis persistence is single-node unless using Redis Cluster or Sentinel) and less sophisticated routing than RabbitMQ, but these limitations are acceptable for event-driven architectures where the outbox pattern already provides delivery guarantees.

### Why Implement Cleanup Workers?

Without cleanup, the outbox table would grow indefinitely as events accumulate, eventually degrading query performance and consuming excessive storage. **Automatic cleanup of sent events** maintains consistent performance by keeping the table size bounded. The 7-day default retention provides a reasonable window for debugging recent relay issues while preventing unbounded growth. Failed events are never automatically deleted, ensuring operational issues are explicitly handled rather than silently discarded. The trade-off is additional worker complexity and the loss of outbox history beyond the retention window, but the business event table retains full history for auditing purposes, making outbox cleanup safe.

---

## Example Integration

An upstream application can publish a domain event to the outbox relay after completing its business operation. Downstream consumers can then react independently:

- An analytics service consumes events to track product metrics and behavior patterns
- A notification service sends emails or push notifications in response to relevant events
- A reporting service aggregates events for business intelligence dashboards
- Other application services can consume events through Redis Streams to create bidirectional event flows

The integration follows this pattern: after an application completes a business operation, it sends an HTTP POST to the outbox relay's `/events` endpoint with an event type, aggregate ID, and domain payload. The relay writes the business event and its outbox record atomically, then publishes it to Redis Streams for consumer groups to process according to their own business logic.

This architecture decouples producers from downstream services. New consumers can be added without modifying producer code. The outbox pattern ensures events are retained even if Redis is temporarily unavailable, creating a reliable and scalable foundation for distributed data consistency.

---

## Setup & Usage

### Prerequisites

- Node.js >= 20.0.0
- PostgreSQL database
- Redis server

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
DATABASE_URL="postgresql://user:password@localhost:5432/outbox_event_relay"
REDIS_URL="redis://localhost:6379"
PORT=3001
NODE_ENV=development
LOG_LEVEL=info
POLL_INTERVAL_MS=1000
BATCH_SIZE=10
MAX_RETRIES=5
STREAM_NAME=business-events
CLEANUP_DAYS=7
```

### Installation & Database Setup

```bash
npm install
npx prisma migrate dev    # Creates database tables
npx prisma generate       # Generates Prisma client
```

### Running the Service

```bash
npm run dev        # Start API server (Terminal 1)
npm run worker     # Start relay worker (Terminal 2)
```

### Testing

```bash
npm test                  # Run all tests
npm run test:integration  # Integration tests only
npm run test:unit         # Unit tests only
```

### API Endpoints

- `POST /events` - Create business event with outbox entry
- `GET /outbox/status` - Get outbox event counts and statistics
- `GET /outbox/pending` - List pending events with pagination
- `GET /outbox/failed` - List failed events for troubleshooting
- `GET /health` - Health check (database and Redis connectivity)
- `GET /metrics` - Prometheus metrics

---

## Observability

The service provides comprehensive observability through structured logging, health checks, and metrics:

- **Structured Logging**: Pino JSON logs with contextual fields (event ID, aggregate ID, status, attempts) for tracing events through the entire pipeline
- **Health Checks**: `/health` endpoint tests actual database and Redis connectivity with latency measurements for load balancer probes
- **Prometheus Metrics**: Event counts by status, processing duration histograms, queue size gauge, error counters, and cleanup metrics for dashboards and alerting

Monitor `outbox_queue_size` to detect relay worker issues or capacity problems. Set alerts on `outbox_relay_duration_seconds` P99 latency to identify performance degradation. Track `redis_publish_errors_total` for connectivity issues requiring infrastructure attention. Use the `/outbox/failed` endpoint to investigate events that exceeded max retry attempts and require manual intervention.

---

**Built for reliability and consistency in distributed systems.**
