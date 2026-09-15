import 'dotenv/config';
import axios from 'axios';
import Redis from 'ioredis';
import { PrismaClient } from '@prisma/client';

const BASE_URL = `http://localhost:${process.env.PORT || 3001}`;
const STREAM_NAME = process.env.STREAM_NAME || 'business-events';

const prisma = new PrismaClient();
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

interface TestResult {
  name: string;
  passed: boolean;
  duration: number;
  error?: string;
}

const results: TestResult[] = [];

function formatDuration(ms: number): string {
  return `${ms.toFixed(0)}ms`;
}

function log(message: string, type: 'info' | 'success' | 'error' | 'warn' = 'info'): void {
  const colors = {
    info: '\x1b[36m',
    success: '\x1b[32m',
    error: '\x1b[31m',
    warn: '\x1b[33m',
  };
  const reset = '\x1b[0m';
  const icon = {
    info: '[INFO]',
    success: '[PASS]',
    error: '[FAIL]',
    warn: '[WARN]',
  };

  console.log(`${colors[type]}${icon[type]}${reset} ${message}`);
}

async function runTest(name: string, testFn: () => Promise<void>): Promise<void> {
  const start = Date.now();
  try {
    await testFn();
    const duration = Date.now() - start;
    results.push({ name, passed: true, duration });
    log(`${name} - ${formatDuration(duration)}`, 'success');
  } catch (error) {
    const duration = Date.now() - start;
    const errorMessage = error instanceof Error ? error.message : String(error);
    results.push({ name, passed: false, duration, error: errorMessage });
    log(`${name} - ${errorMessage}`, 'error');
  }
}

async function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function testHealthEndpoint(): Promise<void> {
  const response = await axios.get(`${BASE_URL}/health`);

  if (response.status !== 200) {
    throw new Error(`Expected status 200, got ${response.status}`);
  }

  if (!response.data.checks.database.healthy) {
    throw new Error('Database health check failed');
  }

  if (!response.data.checks.redis.healthy) {
    throw new Error('Redis health check failed');
  }
}

async function testMetricsEndpoint(): Promise<void> {
  const response = await axios.get(`${BASE_URL}/metrics`);

  if (response.status !== 200) {
    throw new Error(`Expected status 200, got ${response.status}`);
  }

  if (!response.data.includes('outbox_events_total')) {
    throw new Error('Metrics response missing outbox_events_total');
  }
}

async function testEventCreation(): Promise<void> {
  const eventId = `test-${Date.now()}`;
  const payload = {
    eventType: 'test.created',
    aggregateId: eventId,
    payload: {
      testId: eventId,
      timestamp: new Date().toISOString(),
      data: 'test data',
    },
  };

  const response = await axios.post(`${BASE_URL}/events`, payload);

  if (response.status !== 201) {
    throw new Error(`Expected status 201, got ${response.status}`);
  }

  if (!response.data.success) {
    throw new Error('Event creation response indicates failure');
  }

  if (!response.data.data.businessEventId) {
    throw new Error('Missing businessEventId in response');
  }

  if (!response.data.data.outboxEventId) {
    throw new Error('Missing outboxEventId in response');
  }

  const outboxEvent = await prisma.outboxEvent.findUnique({
    where: { id: response.data.data.outboxEventId },
  });

  if (!outboxEvent) {
    throw new Error('Outbox event not found in database');
  }

  if (outboxEvent.status !== 'pending' && outboxEvent.status !== 'sent') {
    throw new Error(`Expected status 'pending' or 'sent', got '${outboxEvent.status}'`);
  }
}

async function testWorkerProcessing(): Promise<void> {
  const eventId = `worker-test-${Date.now()}`;
  const payload = {
    eventType: 'worker.test',
    aggregateId: eventId,
    payload: { testId: eventId },
  };

  const response = await axios.post(`${BASE_URL}/events`, payload);
  const outboxEventId = response.data.data.outboxEventId;

  let processed = false;
  let attempts = 0;
  const maxAttempts = 10;

  while (!processed && attempts < maxAttempts) {
    await wait(1000);
    attempts++;

    const outboxEvent = await prisma.outboxEvent.findUnique({
      where: { id: outboxEventId },
    });

    if (outboxEvent?.status === 'sent') {
      processed = true;
    }
  }

  if (!processed) {
    throw new Error(`Worker did not process event after ${maxAttempts} seconds`);
  }
}

async function testRedisStreamPublishing(): Promise<void> {
  const eventId = `redis-test-${Date.now()}`;
  const payload = {
    eventType: 'redis.test',
    aggregateId: eventId,
    payload: { testId: eventId },
  };

  await axios.post(`${BASE_URL}/events`, payload);

  let found = false;
  let attempts = 0;
  const maxAttempts = 10;

  while (!found && attempts < maxAttempts) {
    await wait(500);
    attempts++;

    const messages = await redis.xrevrange(STREAM_NAME, '+', '-', 'COUNT', 50);

    if (messages && messages.length > 0) {
      found = messages.some((msg: any) => {
        const fields = msg[1];
        for (let i = 0; i < fields.length; i += 2) {
          if (fields[i] === 'aggregateId' && fields[i + 1] === eventId) {
            return true;
          }
        }
        return false;
      });
    }
  }

  if (!found) {
    throw new Error(`Event not found in Redis Stream after ${maxAttempts} attempts`);
  }
}

async function testIdempotency(): Promise<void> {
  const eventId = `idempotency-test-${Date.now()}`;
  const payload = {
    eventType: 'idempotency.test',
    aggregateId: eventId,
    payload: { testId: eventId },
  };

  const response1 = await axios.post(`${BASE_URL}/events`, payload);
  const response2 = await axios.post(`${BASE_URL}/events`, payload);
  const response3 = await axios.post(`${BASE_URL}/events`, payload);

  if (
    response1.data.data.outboxEventId !== response2.data.data.outboxEventId ||
    response1.data.data.outboxEventId !== response3.data.data.outboxEventId
  ) {
    throw new Error('Duplicate events created different outbox entries');
  }

  const count = await prisma.outboxEvent.count({
    where: {
      eventType: 'idempotency.test',
      aggregateId: eventId,
    },
  });

  if (count !== 1) {
    throw new Error(`Expected 1 outbox event, found ${count}`);
  }
}

async function testConcurrentRequests(): Promise<void> {
  const eventId = `concurrent-test-${Date.now()}`;
  const payload = {
    eventType: 'concurrent.test',
    aggregateId: eventId,
    payload: { testId: eventId },
  };

  const requests = Array(5)
    .fill(null)
    .map(() => axios.post(`${BASE_URL}/events`, payload));

  const responses = await Promise.all(requests);

  const uniqueIds = new Set(responses.map((r) => r.data.data.outboxEventId));

  if (uniqueIds.size !== 1) {
    throw new Error(`Expected 1 unique outbox event, got ${uniqueIds.size}`);
  }

  const count = await prisma.outboxEvent.count({
    where: {
      eventType: 'concurrent.test',
      aggregateId: eventId,
    },
  });

  if (count !== 1) {
    throw new Error(`Expected 1 outbox event after concurrent requests, found ${count}`);
  }
}

async function testStatusEndpoint(): Promise<void> {
  const response = await axios.get(`${BASE_URL}/outbox/status`);

  if (response.status !== 200) {
    throw new Error(`Expected status 200, got ${response.status}`);
  }

  const { counts } = response.data.data;

  if (typeof counts.pending !== 'number') {
    throw new Error('Status response missing pending count');
  }

  if (typeof counts.sent !== 'number') {
    throw new Error('Status response missing sent count');
  }

  if (typeof counts.failed !== 'number') {
    throw new Error('Status response missing failed count');
  }

  if (typeof counts.total !== 'number') {
    throw new Error('Status response missing total count');
  }
}

async function testPendingEndpoint(): Promise<void> {
  const response = await axios.get(`${BASE_URL}/outbox/pending?limit=10`);

  if (response.status !== 200) {
    throw new Error(`Expected status 200, got ${response.status}`);
  }

  if (!response.data.data.events) {
    throw new Error('Pending endpoint missing events array');
  }

  if (!response.data.data.pagination) {
    throw new Error('Pending endpoint missing pagination data');
  }
}

async function testFailedEndpoint(): Promise<void> {
  const response = await axios.get(`${BASE_URL}/outbox/failed?limit=10`);

  if (response.status !== 200) {
    throw new Error(`Expected status 200, got ${response.status}`);
  }

  if (!response.data.data.events) {
    throw new Error('Failed endpoint missing events array');
  }

  if (!response.data.data.pagination) {
    throw new Error('Failed endpoint missing pagination data');
  }
}

async function testInvalidPayload(): Promise<void> {
  const invalidPayload = {
    eventType: '',
    aggregateId: 'test',
  };

  try {
    await axios.post(`${BASE_URL}/events`, invalidPayload);
    throw new Error('Expected request to fail with 400, but it succeeded');
  } catch (error: any) {
    if (error.response?.status !== 400) {
      throw new Error(`Expected status 400, got ${error.response?.status || 'no response'}`);
    }
  }
}

async function cleanupTestData(): Promise<void> {
  const streamLength = await redis.xlen(STREAM_NAME);
  if (streamLength > 0) {
    await redis.xtrim(STREAM_NAME, 'MAXLEN', 0);
  }

  await prisma.outboxEvent.deleteMany({
    where: {
      OR: [
        { eventType: { startsWith: 'test.' } },
        { eventType: { startsWith: 'worker.' } },
        { eventType: { startsWith: 'redis.' } },
        { eventType: { startsWith: 'idempotency.' } },
        { eventType: { startsWith: 'concurrent.' } },
      ],
    },
  });

  await prisma.businessEvent.deleteMany({
    where: {
      OR: [
        { eventType: { startsWith: 'test.' } },
        { eventType: { startsWith: 'worker.' } },
        { eventType: { startsWith: 'redis.' } },
        { eventType: { startsWith: 'idempotency.' } },
        { eventType: { startsWith: 'concurrent.' } },
      ],
    },
  });
}

async function main(): Promise<void> {
  console.log('\n=================================================');
  console.log('   OUTBOX EVENT RELAY - TEST SUITE');
  console.log('=================================================\n');

  log('Starting test suite execution...', 'info');
  log(`Target: ${BASE_URL}`, 'info');
  log(`Redis Stream: ${STREAM_NAME}\n`, 'info');

  try {
    log('Running pre-flight checks...', 'info');
    await runTest('Health endpoint check', testHealthEndpoint);
    await runTest('Metrics endpoint check', testMetricsEndpoint);

    log('\nTesting core functionality...', 'info');
    await runTest('Event creation', testEventCreation);
    await runTest('Worker processing', testWorkerProcessing);
    await runTest('Redis Stream publishing', testRedisStreamPublishing);

    log('\nTesting reliability features...', 'info');
    await runTest('Idempotency', testIdempotency);
    await runTest('Concurrent requests', testConcurrentRequests);

    log('\nTesting monitoring endpoints...', 'info');
    await runTest('Status endpoint', testStatusEndpoint);
    await runTest('Pending events endpoint', testPendingEndpoint);
    await runTest('Failed events endpoint', testFailedEndpoint);

    log('\nTesting error handling...', 'info');
    await runTest('Invalid payload rejection', testInvalidPayload);

    log('\nCleaning up test data...', 'info');
    await cleanupTestData();
    log('Test data cleaned', 'success');
  } catch (error) {
    log(`Fatal error during test execution: ${error}`, 'error');
  } finally {
    await prisma.$disconnect();
    await redis.quit();
  }

  console.log('\n=================================================');
  console.log('                  TEST SUMMARY');
  console.log('=================================================\n');

  const passed = results.filter((r) => r.passed).length;
  const failed = results.filter((r) => !r.passed).length;
  const total = results.length;
  const totalDuration = results.reduce((sum, r) => sum + r.duration, 0);

  console.log(`Total Tests: ${total}`);
  log(`Passed: ${passed}`, passed === total ? 'success' : 'info');
  if (failed > 0) {
    log(`Failed: ${failed}`, 'error');
  }
  console.log(`Total Duration: ${formatDuration(totalDuration)}\n`);

  if (failed > 0) {
    console.log('Failed Tests:');
    results
      .filter((r) => !r.passed)
      .forEach((r) => {
        log(`  - ${r.name}: ${r.error}`, 'error');
      });
    console.log('');
  }

  console.log('=================================================\n');

  process.exit(failed > 0 ? 1 : 0);
}

main();
