import express, { Request, Response, NextFunction } from 'express';
import pinoHttp from 'pino-http';
import { config } from './utils/config';
import { logger } from './utils/logger';
import eventsRouter from './routes/events';
import healthRouter from './routes/health';
import metricsRouter from './routes/metrics';
import outboxRouter from './routes/outbox';

const app = express();

const isDevelopment = config.server.nodeEnv === 'development';

app.use(
  pinoHttp({
    level: config.logging.level,
    transport: isDevelopment
      ? {
          target: 'pino-pretty',
          options: {
            colorize: true,
            translateTime: 'SYS:standard',
            ignore: 'pid,hostname',
          },
        }
      : undefined,
    customLogLevel: (_req, res) => {
      if (res.statusCode >= 500) return 'error';
      if (res.statusCode >= 400) return 'warn';
      return 'info';
    },
  })
);

app.use(express.json());

app.get('/', (_req: Request, res: Response) => {
  res.json({
    service: 'outbox-event-relay',
    version: '1.0.0',
    status: 'running',
  });
});

app.use('/events', eventsRouter);
app.use('/health', healthRouter);
app.use('/metrics', metricsRouter);
app.use('/outbox', outboxRouter);

app.use((_req: Request, res: Response) => {
  res.status(404).json({
    error: 'NOT_FOUND',
    message: 'Endpoint not found',
  });
});

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error({ error: err }, 'UNHANDLED ERROR');

  res.status(500).json({
    error: 'INTERNAL_ERROR',
    message: 'An unexpected error occurred',
  });
});

if (require.main === module) {
  const server = app.listen(config.server.port, () => {
    logger.info(
      {
        port: config.server.port,
        nodeEnv: config.server.nodeEnv,
      },
      'SERVER STARTED'
    );
  });

  const gracefulShutdown = async () => {
    logger.info('SHUTTING DOWN SERVER');

    server.close(() => {
      logger.info('SERVER CLOSED');
      process.exit(0);
    });

    setTimeout(() => {
      logger.error('FORCEFULLY SHUTTING DOWN SERVER');
      process.exit(1);
    }, 10000);
  };

  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGINT', gracefulShutdown);

  process.on('unhandledRejection', (reason, promise) => {
    logger.error({ reason, promise }, 'UNHANDLED PROMISE REJECTION');
  });

  process.on('uncaughtException', (error) => {
    logger.fatal({ error }, 'UNCAUGHT EXCEPTION');
    process.exit(1);
  });
}

export default app;
