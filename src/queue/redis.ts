import Redis from 'ioredis';
import { config } from '../utils/config';
import { logger } from '../utils/logger';

const redisClientSingleton = () => {
  if (config.server.nodeEnv === 'test') {
    const mockClient = {
      status: 'ready',
      quit: async () => {},
      disconnect: async () => {},
    } as any;
    return mockClient;
  }

  const client = new Redis(config.redis.url, {
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
  });

  client.on('connect', () => {
    logger.info('REDIS CLIENT CONNECTED');
  });

  client.on('ready', () => {
    logger.info('REDIS CLIENT READY');
  });

  client.on('error', (error) => {
    logger.error({ error: error.message }, 'REDIS CLIENT ERROR');
  });

  client.on('close', () => {
    logger.warn('REDIS CLIENT CONNECTION CLOSED');
  });

  client.on('reconnecting', () => {
    logger.info('REDIS CLIENT RECONNECTING');
  });

  return client;
};

declare global {
  var redisGlobal: undefined | ReturnType<typeof redisClientSingleton>;
}

const redisClient = globalThis.redisGlobal ?? redisClientSingleton();

if (process.env.NODE_ENV !== 'production') {
  globalThis.redisGlobal = redisClient;
}

export { redisClient };
