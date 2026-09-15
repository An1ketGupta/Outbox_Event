import { Client } from 'pg';
import { config } from '../utils/config';
import { logger } from '../utils/logger';

export class PostgresNotifier {
  private client: Client | null = null;
  private isListening = false;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private eventHandlers: Set<() => void> = new Set();

  async connect(): Promise<void> {
    try {
      this.client = new Client({
        connectionString: config.database.url,
      });

      await this.client.connect();

      this.client.on('notification', (msg) => {
        if (msg.channel === 'outbox_event') {
          logger.debug({ channel: msg.channel }, 'RECEIVED NOTIFY');
          this.eventHandlers.forEach((handler) => handler());
        }
      });

      this.client.on('error', (error) => {
        logger.error({ error: error.message }, 'POSTGRES NOTIFY CLIENT ERROR');
        this.scheduleReconnect();
      });

      this.client.on('end', () => {
        logger.warn('POSTGRES NOTIFY CLIENT DISCONNECTED');
        this.isListening = false;
        this.scheduleReconnect();
      });

      await this.client.query('LISTEN outbox_event');
      this.isListening = true;

      logger.info('POSTGRES NOTIFY CLIENT CONNECTED AND LISTENING');
    } catch (error) {
      logger.error({ error }, 'FAILED TO CONNECT NOTIFY CLIENT');
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimeout) {
      return;
    }

    logger.info('SCHEDULING NOTIFY CLIENT RECONNECTION');

    this.reconnectTimeout = setTimeout(() => {
      this.reconnectTimeout = null;
      this.connect();
    }, 5000);
  }

  onEvent(handler: () => void): void {
    this.eventHandlers.add(handler);
  }

  async disconnect(): Promise<void> {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    if (this.client) {
      try {
        await this.client.end();
      } catch (error) {
        logger.error({ error }, 'ERROR DISCONNECTING NOTIFY CLIENT');
      }
      this.client = null;
    }

    this.isListening = false;
    this.eventHandlers.clear();

    logger.info('POSTGRES NOTIFY CLIENT DISCONNECTED');
  }

  isConnected(): boolean {
    return this.isListening;
  }
}

export const notifier = new PostgresNotifier();
