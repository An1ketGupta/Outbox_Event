import request from 'supertest';
import app from '../../src/server';
import { prisma } from '../../src/db';

describe('POST /events', () => {
  it('should create a business event and outbox entry', async () => {
    const payload = {
      eventType: 'user.created',
      aggregateId: 'user-123',
      payload: {
        userId: 'user-123',
        email: 'test@example.com',
        name: 'Test User',
      },
    };

    const response = await request(app).post('/events').send(payload).expect(201);

    expect(response.body).toMatchObject({
      success: true,
      message: 'Event created and queued for publishing',
      data: {
        eventType: 'user.created',
        aggregateId: 'user-123',
      },
    });

    expect(response.body.data.businessEventId).toBeTruthy();
    expect(response.body.data.outboxEventId).toBeTruthy();

    const outboxEvent = await prisma.outboxEvent.findUnique({
      where: { id: response.body.data.outboxEventId },
    });

    expect(outboxEvent).toBeTruthy();
    expect(outboxEvent?.status).toBe('pending');
    expect(outboxEvent?.eventType).toBe('user.created');
  });

  it('should handle duplicate events idempotently', async () => {
    const payload = {
      eventType: 'payment.completed',
      aggregateId: 'payment-456',
      payload: {
        amount: 10000,
        currency: 'USD',
      },
    };

    const firstResponse = await request(app).post('/events').send(payload).expect(201);

    const secondResponse = await request(app).post('/events').send(payload).expect(201);

    expect(firstResponse.body.data.outboxEventId).toBe(secondResponse.body.data.outboxEventId);

    const outboxCount = await prisma.outboxEvent.count({
      where: {
        eventType: 'payment.completed',
        aggregateId: 'payment-456',
      },
    });

    expect(outboxCount).toBe(1);
  });

  it('should reject invalid payload', async () => {
    const invalidPayload = {
      eventType: '',
      aggregateId: 'test',
    };

    const response = await request(app).post('/events').send(invalidPayload).expect(400);

    expect(response.body).toMatchObject({
      success: false,
      error: 'INVALID_PAYLOAD',
    });
  });

  it('should reject missing required fields', async () => {
    const incompletePayload = {
      eventType: 'test.event',
    };

    await request(app).post('/events').send(incompletePayload).expect(400);
  });
});
