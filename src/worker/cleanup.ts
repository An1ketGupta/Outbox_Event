import { prisma } from '../db';
import { config } from '../utils/config';
import { logger } from '../utils/logger';
import { outboxCleanupTotal } from '../routes/metrics';

export class CleanupWorker {
  private isRunning = false;
  private intervalId: NodeJS.Timeout | null = null;

  async cleanupOldEvents(): Promise<number> {
    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - config.cleanup.days);

      logger.info(
        { cutoffDate: cutoffDate.toISOString(), days: config.cleanup.days },
        'STARTING CLEANUP OF OLD SENT EVENTS'
      );

      const result = await prisma.outboxEvent.deleteMany({
        where: {
          status: 'sent',
          sentAt: {
            lt: cutoffDate,
          },
        },
      });

      if (result.count > 0) {
        outboxCleanupTotal.inc(result.count);
      }

      logger.info(
        {
          deletedCount: result.count,
          cutoffDate: cutoffDate.toISOString(),
        },
        'CLEANUP COMPLETED'
      );

      return result.count;
    } catch (error) {
      logger.error({ error }, 'CLEANUP JOB FAILED');
      return 0;
    }
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      logger.warn('CLEANUP WORKER ALREADY RUNNING');
      return;
    }

    this.isRunning = true;

    const intervalMs = 24 * 60 * 60 * 1000;

    logger.info(
      {
        intervalHours: 24,
        cleanupDays: config.cleanup.days,
      },
      'CLEANUP WORKER STARTED'
    );

    await this.cleanupOldEvents();

    this.intervalId = setInterval(async () => {
      await this.cleanupOldEvents();
    }, intervalMs);
  }

  stop(): void {
    if (!this.isRunning) {
      return;
    }

    logger.info('STOPPING CLEANUP WORKER');

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }

    this.isRunning = false;

    logger.info('CLEANUP WORKER STOPPED');
  }
}

export const cleanupWorker = new CleanupWorker();
