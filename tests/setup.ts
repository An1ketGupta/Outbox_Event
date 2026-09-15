import { prisma } from '../src/db';
import { redisClient } from '../src/queue/redis';

beforeAll(async () => {
  await prisma.$connect();
});

afterAll(async () => {
  await prisma.$disconnect();
  if (redisClient && typeof redisClient.quit === 'function') {
    try {
      await redisClient.quit();
    } catch (error) {
    }
  }
});

afterEach(async () => {
  await prisma.outboxEvent.deleteMany();
  await prisma.businessEvent.deleteMany();
});
