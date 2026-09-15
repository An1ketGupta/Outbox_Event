import { Router, Request, Response } from 'express';
import { register, collectDefaultMetrics, Counter, Histogram, Gauge } from 'prom-client';
import { logger } from '../utils/logger';

const router = Router();

collectDefaultMetrics({ register });

export const outboxEventsTotal = new Counter({
  name: 'outbox_events_total',
  help: 'Total number of outbox events by status',
  labelNames: ['status'] as const,
  registers: [register],
});

export const outboxRelayDuration = new Histogram({
  name: 'outbox_relay_duration_seconds',
  help: 'Duration of outbox relay processing in seconds',
  labelNames: ['status'] as const,
  buckets: [0.1, 0.5, 1, 2, 5, 10],
  registers: [register],
});

export const outboxQueueSize = new Gauge({
  name: 'outbox_queue_size',
  help: 'Current number of pending events in outbox',
  registers: [register],
});

export const redisPublishErrors = new Counter({
  name: 'redis_publish_errors_total',
  help: 'Total number of Redis publish errors',
  labelNames: ['error_type'] as const,
  registers: [register],
});

export const outboxCleanupTotal = new Counter({
  name: 'outbox_cleanup_events_total',
  help: 'Total number of events cleaned up from outbox',
  registers: [register],
});

router.get('/', async (_req: Request, res: Response) => {
  try {
    res.set('Content-Type', register.contentType);
    const metrics = await register.metrics();
    res.end(metrics);
  } catch (error) {
    logger.error({ error }, 'FAILED TO GENERATE METRICS');
    res.status(500).json({
      error: 'METRICS_ERROR',
      message: 'Failed to generate metrics',
    });
  }
});

export default router;
