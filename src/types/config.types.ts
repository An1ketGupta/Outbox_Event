export interface Config {
  database: {
    url: string;
  };
  redis: {
    url: string;
  };
  server: {
    port: number;
    nodeEnv: 'development' | 'production' | 'test';
  };
  worker: {
    pollIntervalMs: number;
    batchSize: number;
    maxRetries: number;
  };
  streams: {
    name: string;
  };
  logging: {
    level: 'fatal' | 'error' | 'warn' | 'info' | 'debug' | 'trace';
  };
  cleanup: {
    days: number;
  };
}
