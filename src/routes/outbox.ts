import { Router, Request, Response } from 'express';
import { prisma } from '../db';
import { logger } from '../utils/logger';

const router = Router();

router.get('/status', async (_req: Request, res: Response) => {
  try {
    const [pending, sent, failed, total] = await Promise.all([
      prisma.outboxEvent.count({ where: { status: 'pending' } }),
      prisma.outboxEvent.count({ where: { status: 'sent' } }),
      prisma.outboxEvent.count({ where: { status: 'failed' } }),
      prisma.outboxEvent.count(),
    ]);

    const oldestPending = await prisma.outboxEvent.findFirst({
      where: { status: 'pending' },
      orderBy: { createdAt: 'asc' },
      select: { createdAt: true },
    });

    return res.json({
      status: 'success',
      data: {
        counts: {
          pending,
          sent,
          failed,
          total,
        },
        oldestPendingAge: oldestPending
          ? Date.now() - oldestPending.createdAt.getTime()
          : null,
        timestamp: new Date().toISOString(),
      },
    });
  } catch (error) {
    logger.error({ error }, 'FAILED TO GET OUTBOX STATUS');
    return res.status(500).json({
      error: 'INTERNAL_ERROR',
      message: 'Failed to retrieve outbox status',
    });
  }
});

router.get('/failed', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 100, 1000);
    const offset = parseInt(req.query.offset as string) || 0;
    const eventType = req.query.eventType as string | undefined;

    const where = {
      status: 'failed',
      ...(eventType && { eventType }),
    };

    const [events, total] = await Promise.all([
      prisma.outboxEvent.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        take: limit,
        skip: offset,
        select: {
          id: true,
          aggregateId: true,
          eventType: true,
          status: true,
          attempts: true,
          error: true,
          createdAt: true,
          sentAt: true,
        },
      }),
      prisma.outboxEvent.count({ where }),
    ]);

    return res.json({
      status: 'success',
      data: {
        events,
        pagination: {
          total,
          limit,
          offset,
          hasMore: offset + limit < total,
        },
      },
    });
  } catch (error) {
    logger.error({ error }, 'FAILED TO GET FAILED EVENTS');
    return res.status(500).json({
      error: 'INTERNAL_ERROR',
      message: 'Failed to retrieve failed events',
    });
  }
});

router.get('/pending', async (req: Request, res: Response) => {
  try {
    const limit = Math.min(parseInt(req.query.limit as string) || 100, 1000);
    const offset = parseInt(req.query.offset as string) || 0;
    const eventType = req.query.eventType as string | undefined;

    const where = {
      status: 'pending',
      ...(eventType && { eventType }),
    };

    const [events, total] = await Promise.all([
      prisma.outboxEvent.findMany({
        where,
        orderBy: { createdAt: 'asc' },
        take: limit,
        skip: offset,
        select: {
          id: true,
          aggregateId: true,
          eventType: true,
          status: true,
          attempts: true,
          error: true,
          createdAt: true,
        },
      }),
      prisma.outboxEvent.count({ where }),
    ]);

    return res.json({
      status: 'success',
      data: {
        events,
        pagination: {
          total,
          limit,
          offset,
          hasMore: offset + limit < total,
        },
      },
    });
  } catch (error) {
    logger.error({ error }, 'FAILED TO GET PENDING EVENTS');
    return res.status(500).json({
      error: 'INTERNAL_ERROR',
      message: 'Failed to retrieve pending events',
    });
  }
});

export default router;
