import { z } from 'zod';
import { Prisma } from '@prisma/client';
import { prisma } from '../db';
import { logger } from '../utils/logger';
import type { CreateEventParams, CreateEventResult } from '../types/event.types';

const createEventParamsSchema = z.object({
  eventType: z.string().min(1).max(100),
  aggregateId: z.string().min(1).max(255),
  payload: z.any(),
});

export class EventService {
  async createBusinessEvent(params: CreateEventParams): Promise<CreateEventResult> {
    const validatedParams = createEventParamsSchema.parse(params);
    const { eventType, aggregateId, payload } = validatedParams;

    logger.info(
      { eventType, aggregateId },
      'CREATING BUSINESS EVENT WITH OUTBOX ENTRY'
    );

    try {
      const result = await prisma.$transaction(async (tx) => {
        const businessEvent = await tx.businessEvent.create({
          data: {
            eventType,
            aggregateId,
            payload: payload as Prisma.JsonObject,
          },
        });

        const outboxEvent = await tx.outboxEvent.create({
          data: {
            eventType,
            aggregateId,
            payload: payload as Prisma.JsonObject,
            status: 'pending',
          },
        });

        await tx.$executeRaw`NOTIFY outbox_event`;

        return {
          businessEventId: businessEvent.id,
          outboxEventId: outboxEvent.id,
          eventType: businessEvent.eventType,
          aggregateId: businessEvent.aggregateId,
        };
      });

      logger.info(
        {
          businessEventId: result.businessEventId,
          outboxEventId: result.outboxEventId,
          eventType: result.eventType,
        },
        'BUSINESS EVENT AND OUTBOX ENTRY CREATED'
      );

      return result;
    } catch (error) {
      if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002') {
        logger.warn(
          { eventType, aggregateId },
          'DUPLICATE EVENT DETECTED'
        );

        const existingOutbox = await prisma.outboxEvent.findUnique({
          where: {
            aggregateId_eventType: {
              aggregateId,
              eventType,
            },
          },
        });

        if (existingOutbox) {
          return {
            businessEventId: '',
            outboxEventId: existingOutbox.id,
            eventType: existingOutbox.eventType,
            aggregateId: existingOutbox.aggregateId,
          };
        }
      }

      logger.error({ error, eventType, aggregateId }, 'FAILED TO CREATE BUSINESS EVENT');
      throw error;
    }
  }
}

export const eventService = new EventService();
