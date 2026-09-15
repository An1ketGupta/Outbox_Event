import request from 'supertest';
import app from '../../src/server';
import { prisma } from '../../src/db';

describe('Idempotency Tests', () => {
  it('should prevent duplicate outbox entries for same event', async () => {
    const payload = {
      eventType: 'subscription.renewed',
      aggregateId: 'sub-12345',
      payload: {
        subscriptionId: 'sub-12345',
        plan: 'premium',
        price: 2999,
      },
    };

    await request(app).post('/events').send(payload).expect(201);

    await request(app).post('/events').send(payload).expect(201);

    await request(app).post('/events').send(payload).expect(201);

    const outboxCount = await prisma.outboxEvent.count({
      where: {
        eventType: 'subscription.renewed',
        aggregateId: 'sub-12345',
      },
    });

    expect(outboxCount).toBe(1);

    const businessEventCount = await prisma.businessEvent.count({
      where: {
        eventType: 'subscription.renewed',
        aggregateId: 'sub-12345',
      },
    });

    expect(businessEventCount).toBeGreaterThanOrEqual(1);
  });

  it('should allow same eventType with different aggregateId', async () => {
    const basePayload = {
      eventType: 'invoice.generated',
      payload: { amount: 5000 },
    };

    await request(app)
      .post('/events')
      .send({ ...basePayload, aggregateId: 'invoice-001' })
      .expect(201);

    await request(app)
      .post('/events')
      .send({ ...basePayload, aggregateId: 'invoice-002' })
      .expect(201);

    const outboxCount = await prisma.outboxEvent.count({
      where: { eventType: 'invoice.generated' },
    });

    expect(outboxCount).toBe(2);
  });

  it('should allow different eventTypes for same aggregateId', async () => {
    const aggregateId = 'user-777';

    await request(app)
      .post('/events')
      .send({
        eventType: 'user.created',
        aggregateId,
        payload: { email: 'user@example.com' },
      })
      .expect(201);

    await request(app)
      .post('/events')
      .send({
        eventType: 'user.verified',
        aggregateId,
        payload: { verifiedAt: new Date().toISOString() },
      })
      .expect(201);

    const outboxCount = await prisma.outboxEvent.count({
      where: { aggregateId },
    });

    expect(outboxCount).toBe(2);
  });

  it('should maintain idempotency across concurrent requests', async () => {
    const payload = {
      eventType: 'payment.captured',
      aggregateId: 'payment-concurrent-test',
      payload: { amount: 15000 },
    };

    const requests = Array(10)
      .fill(null)
      .map(() => request(app).post('/events').send(payload));

    const responses = await Promise.all(requests);

    responses.forEach((response) => {
      expect([201]).toContain(response.status);
    });

    const outboxCount = await prisma.outboxEvent.count({
      where: {
        eventType: 'payment.captured',
        aggregateId: 'payment-concurrent-test',
      },
    });

    expect(outboxCount).toBe(1);
  });
});
