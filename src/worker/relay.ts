import { prisma } from '../db';
import { notifier } from '../db/notify';
import { redisClient } from '../queue/redis';
import { redisPublisher } from '../queue/publisher';
import { config } from '../utils/config';
import { logger } from '../utils/logger';
import { cleanupWorker } from './cleanup';
import {
  outboxEventsTotal,
  outboxRelayDuration,
  outboxQueueSize,
  redisPublishErrors,
} from '../routes/metrics';
import type { RelayJob, RelayResult } from '../types/worker.types';

class OutboxRelayWorker {
  private isRunning = false;
  private intervalId: NodeJS.Timeout | null = null;
  private isProcessing = false;

  async processOutboxBatch(): Promise<RelayResult> {
    if (this.isProcessing) {
      return { processed: 0, succeeded: 0, failed: 0 };
    }

    this.isProcessing = true;

    const result: RelayResult = {
      processed: 0,
      succeeded: 0,
      failed: 0,
    };

    const timer = outboxRelayDuration.startTimer();

    try {
      const events = await prisma.$queryRaw<RelayJob[]>`
        SELECT id, "aggregateId", "eventType", payload, attempts
        FROM "OutboxEvent"
        WHERE status = 'pending'
        AND attempts < ${config.worker.maxRetries}
        ORDER BY "createdAt" ASC
        LIMIT ${config.worker.batchSize}
        FOR UPDATE SKIP LOCKED
      `;

      const queueSize = await prisma.outboxEvent.count({
        where: {
          status: 'pending',
        },
      });
      outboxQueueSize.set(queueSize);

      if (events.length === 0) {
        timer({ status: 'success' });
        return result;
      }

      logger.info({ count: events.length }, 'PROCESSING OUTBOX BATCH');

      for (const event of events) {
        result.processed++;

        try {
          await redisPublisher.publishToStream({
            eventId: event.id,
            eventType: event.eventType,
            aggregateId: event.aggregateId,
            payload: event.payload,
          });

          await prisma.outboxEvent.update({
            where: { id: event.id },
            data: {
              status: 'sent',
              sentAt: new Date(),
              attempts: event.attempts + 1,
            },
          });

          result.succeeded++;
          outboxEventsTotal.inc({ status: 'sent' });

          logger.info(
            { eventId: event.id, eventType: event.eventType },
            'OUTBOX EVENT PUBLISHED SUCCESSFULLY'
          );
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          const newAttempts = event.attempts + 1;
          const newStatus = newAttempts >= config.worker.maxRetries ? 'failed' : 'pending';

          await prisma.outboxEvent.update({
            where: { id: event.id },
            data: {
              status: newStatus,
              attempts: newAttempts,
              error: errorMessage,
            },
          });

          result.failed++;
          outboxEventsTotal.inc({ status: newStatus });
          redisPublishErrors.inc({ error_type: 'publish_failed' });

          logger.error(
            {
              eventId: event.id,
              eventType: event.eventType,
              attempts: newAttempts,
              maxRetries: config.worker.maxRetries,
              error: errorMessage,
            },
            'OUTBOX EVENT PUBLISH FAILED'
          );
        }
      }

      logger.info(
        {
          processed: result.processed,
          succeeded: result.succeeded,
          failed: result.failed,
        },
        'OUTBOX BATCH PROCESSING COMPLETE'
      );

      timer({ status: 'success' });
    } catch (error) {
      logger.error({ error }, 'OUTBOX BATCH PROCESSING ERROR');
      timer({ status: 'error' });
    } finally {
      this.isProcessing = false;
    }

    return result;
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn('RELAY WORKER ALREADY RUNNING');
      return;
    }

    this.isRunning = true;

    logger.info(
      {
        pollIntervalMs: config.worker.pollIntervalMs,
        batchSize: config.worker.batchSize,
        maxRetries: config.worker.maxRetries,
      },
      'RELAY WORKER STARTED'
    );

    await notifier.connect();

    notifier.onEvent(() => {
      this.processOutboxBatch();
    });

    this.intervalId = setInterval(async () => {
      await this.processOutboxBatch();
    }, config.worker.pollIntervalMs);

    await this.processOutboxBatch();

    await cleanupWorker.start();
  }

  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    logger.info('STOPPING RELAY WORKER');

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }

    this.isRunning = false;

    cleanupWorker.stop();
    await notifier.disconnect();
    await prisma.$disconnect();
    await redisClient.quit();

    logger.info('RELAY WORKER STOPPED');
  }
}

const worker = new OutboxRelayWorker();

const gracefulShutdown = async () => {
  await worker.stop();
  process.exit(0);
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

worker.start().catch((error) => {
  logger.fatal({ error }, 'FAILED TO START RELAY WORKER');
  process.exit(1);
});
