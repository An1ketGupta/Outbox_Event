import { eventService } from '../../src/services/eventService';
import { prisma } from '../../src/db';

describe('EventService', () => {
  describe('createBusinessEvent', () => {
    it('should create business event and outbox entry atomically', async () => {
      const result = await eventService.createBusinessEvent({
        eventType: 'order.placed',
        aggregateId: 'order-789',
        payload: {
          orderId: 'order-789',
          items: ['item1', 'item2'],
          total: 5000,
        },
      });

      expect(result.eventType).toBe('order.placed');
      expect(result.aggregateId).toBe('order-789');
      expect(result.businessEventId).toBeTruthy();
      expect(result.outboxEventId).toBeTruthy();

      const businessEvent = await prisma.businessEvent.findUnique({
        where: { id: result.businessEventId },
      });

      const outboxEvent = await prisma.outboxEvent.findUnique({
        where: { id: result.outboxEventId },
      });

      expect(businessEvent).toBeTruthy();
      expect(outboxEvent).toBeTruthy();
      expect(businessEvent?.eventType).toBe('order.placed');
      expect(outboxEvent?.status).toBe('pending');
    });

    it('should handle duplicate events and return existing outbox entry', async () => {
      const params = {
        eventType: 'refund.processed',
        aggregateId: 'refund-999',
        payload: { amount: 1000 },
      };

      const firstResult = await eventService.createBusinessEvent(params);
      const secondResult = await eventService.createBusinessEvent(params);

      expect(firstResult.outboxEventId).toBe(secondResult.outboxEventId);

      const outboxCount = await prisma.outboxEvent.count({
        where: {
          eventType: 'refund.processed',
          aggregateId: 'refund-999',
        },
      });

      expect(outboxCount).toBe(1);
    });

    it('should validate event parameters', async () => {
      await expect(
        eventService.createBusinessEvent({
          eventType: '',
          aggregateId: 'test',
          payload: {},
        })
      ).rejects.toThrow();
    });

    it('should validate aggregateId length', async () => {
      await expect(
        eventService.createBusinessEvent({
          eventType: 'test.event',
          aggregateId: '',
          payload: {},
        })
      ).rejects.toThrow();
    });
  });
});
