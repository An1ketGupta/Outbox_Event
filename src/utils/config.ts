import 'dotenv/config';
import { z } from 'zod';
import type { Config } from '../types/config.types';

const configSchema = z.object({
  database: z.object({
    url: z.string().url(),
  }),
  redis: z.object({
    url: z.string().url(),
  }),
  server: z.object({
    port: z.coerce.number().int().positive().default(3001),
    nodeEnv: z.enum(['development', 'production', 'test']).default('development'),
  }),
  worker: z.object({
    pollIntervalMs: z.coerce.number().int().positive().default(1000),
    batchSize: z.coerce.number().int().positive().default(10),
    maxRetries: z.coerce.number().int().positive().default(5),
  }),
  streams: z.object({
    name: z.string().default('business-events'),
  }),
  logging: z.object({
    level: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  }),
  cleanup: z.object({
    days: z.coerce.number().int().positive().default(7),
  }),
});

function loadConfig(): Config {
  const rawConfig = {
    database: {
      url: process.env.DATABASE_URL,
    },
    redis: {
      url: process.env.REDIS_URL,
    },
    server: {
      port: process.env.PORT,
      nodeEnv: process.env.NODE_ENV,
    },
    worker: {
      pollIntervalMs: process.env.POLL_INTERVAL_MS,
      batchSize: process.env.BATCH_SIZE,
      maxRetries: process.env.MAX_RETRIES,
    },
    streams: {
      name: process.env.STREAM_NAME,
    },
    logging: {
      level: process.env.LOG_LEVEL,
    },
    cleanup: {
      days: process.env.CLEANUP_DAYS,
    },
  };

  const result = configSchema.safeParse(rawConfig);

  if (!result.success) {
    console.error('CONFIGURATION VALIDATION FAILED:', result.error.format());
    process.exit(1);
  }

  return result.data;
}

export const config = loadConfig();
