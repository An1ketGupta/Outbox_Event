export interface BusinessEventPayload {
  [key: string]: unknown;
}

export interface CreateEventParams {
  eventType: string;
  aggregateId: string;
  payload: BusinessEventPayload;
}

export interface CreateEventResult {
  businessEventId: string;
  outboxEventId: number;
  eventType: string;
  aggregateId: string;
}

export interface OutboxEventStatus {
  id: number;
  aggregateId: string;
  eventType: string;
  status: string;
  attempts: number;
  error: string | null;
  createdAt: Date;
  sentAt: Date | null;
}
