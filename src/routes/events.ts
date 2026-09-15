import { Router, Request, Response } from 'express';
import { z } from 'zod';
import { eventService } from '../services/eventService';
import { logger } from '../utils/logger';

const router = Router();

const createEventSchema = z.object({
  eventType: z.string().min(1).max(100),
  aggregateId: z.string().min(1).max(255),
  payload: z.any(),
});

router.post('/', async (req: Request, res: Response) => {
  try {
    const validationResult = createEventSchema.safeParse(req.body);

    if (!validationResult.success) {
      logger.warn({ errors: validationResult.error.format() }, 'INVALID EVENT PAYLOAD');
      return res.status(400).json({
        success: false,
        error: 'INVALID_PAYLOAD',
        message: 'Invalid event payload format',
        details: validationResult.error.format(),
      });
    }

    const { eventType, aggregateId, payload } = validationResult.data;

    const result = await eventService.createBusinessEvent({
      eventType,
      aggregateId,
      payload,
    });

    logger.info(
      {
        businessEventId: result.businessEventId,
        outboxEventId: result.outboxEventId,
      },
      'EVENT CREATED SUCCESSFULLY'
    );

    return res.status(201).json({
      success: true,
      message: 'Event created and queued for publishing',
      data: {
        businessEventId: result.businessEventId,
        outboxEventId: result.outboxEventId,
        eventType: result.eventType,
        aggregateId: result.aggregateId,
      },
    });
  } catch (error) {
    logger.error({ error }, 'EVENT CREATION FAILED');

    return res.status(500).json({
      success: false,
      error: 'INTERNAL_ERROR',
      message: 'Failed to create event',
    });
  }
});

export default router;
