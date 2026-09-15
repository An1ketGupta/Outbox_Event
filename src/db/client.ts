import { PrismaClient, Prisma } from '@prisma/client';
import { logger } from '../utils/logger';

const prismaClientSingleton = () => {
  return new PrismaClient({
    log: [
      { emit: 'event', level: 'query' },
      { emit: 'event', level: 'error' },
      { emit: 'event', level: 'warn' },
    ],
  });
};

declare global {
  var prismaGlobal: undefined | ReturnType<typeof prismaClientSingleton>;
}

const prisma = globalThis.prismaGlobal ?? prismaClientSingleton();

prisma.$on('query', (e: Prisma.QueryEvent) => {
  logger.debug({ query: e.query, duration: e.duration }, 'DATABASE QUERY');
});

prisma.$on('error', (e: Prisma.LogEvent) => {
  logger.error({ target: e.target, message: e.message }, 'DATABASE ERROR');
});

prisma.$on('warn', (e: Prisma.LogEvent) => {
  logger.warn({ target: e.target, message: e.message }, 'DATABASE WARNING');
});

if (process.env.NODE_ENV !== 'production') {
  globalThis.prismaGlobal = prisma;
}

export { prisma };
