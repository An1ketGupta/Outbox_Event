export interface RelayJob {
  id: number;
  aggregateId: string;
  eventType: string;
  payload: Record<string, unknown>;
  attempts: number;
}

export interface RelayResult {
  processed: number;
  succeeded: number;
  failed: number;
}
