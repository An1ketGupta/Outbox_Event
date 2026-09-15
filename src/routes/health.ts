import { Router, Request, Response } from 'express';
import { prisma } from '../db';
import { redisClient } from '../queue/redis';
import { logger } from '../utils/logger';

const router = Router();

router.get('/', async (_req: Request, res: Response) => {
  const checks = {
    database: { healthy: false, latency: 0 },
    redis: { healthy: false, latency: 0 },
  };

  try {
    const dbStart = Date.now();
    await prisma.$queryRaw`SELECT 1`;
    checks.database.healthy = true;
    checks.database.latency = Date.now() - dbStart;
  } catch (error) {
    logger.error({ error }, 'DATABASE HEALTH CHECK FAILED');
  }

  try {
    const redisStart = Date.now();
    await redisClient.ping();
    checks.redis.healthy = true;
    checks.redis.latency = Date.now() - redisStart;
  } catch (error) {
    logger.error({ error }, 'REDIS HEALTH CHECK FAILED');
  }

  const allHealthy = checks.database.healthy && checks.redis.healthy;
  const status = allHealthy ? 200 : 503;

  return res.status(status).json({
    status: allHealthy ? 'healthy' : 'unhealthy',
    timestamp: new Date().toISOString(),
    checks,
  });
});

export default router;
