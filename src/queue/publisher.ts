import { redisClient } from './redis';
import { config } from '../utils/config';
import { logger } from '../utils/logger';

export interface PublishEventParams {
  eventId: number;
  eventType: string;
  aggregateId: string;
  payload: Record<string, unknown>;
}

export class RedisPublisher {
  private async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async publishToStream(params: PublishEventParams): Promise<string> {
    const { eventId, eventType, aggregateId, payload } = params;
    const streamName = config.streams.name;

    let lastError: Error | null = null;
    const maxRetries = 3;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        logger.info(
          { eventId, eventType, aggregateId, attempt, streamName },
          'PUBLISHING EVENT TO REDIS STREAM'
        );

        const messageId = await redisClient.xadd(
          streamName,
          '*',
          'eventId',
          eventId.toString(),
          'eventType',
          eventType,
          'aggregateId',
          aggregateId,
          'payload',
          JSON.stringify(payload),
          'timestamp',
          new Date().toISOString()
        );

        if (!messageId) {
          throw new Error('XADD returned null message ID');
        }

        logger.info(
          { eventId, eventType, messageId, streamName },
          'EVENT PUBLISHED TO REDIS STREAM'
        );

        return messageId;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));

        logger.warn(
          {
            eventId,
            eventType,
            attempt,
            maxRetries,
            error: lastError.message,
          },
          'FAILED TO PUBLISH TO REDIS STREAM'
        );

        if (attempt < maxRetries) {
          const backoffMs = Math.pow(2, attempt) * 1000;
          logger.info({ backoffMs, attempt }, 'RETRYING AFTER BACKOFF');
          await this.delay(backoffMs);
        }
      }
    }

    logger.error(
      {
        eventId,
        eventType,
        error: lastError?.message,
      },
      'EXHAUSTED RETRIES FOR REDIS STREAM PUBLISH'
    );

    throw lastError;
  }
}

export const redisPublisher = new RedisPublisher();
