import express, { Application } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import { errorHandler } from './middlewares/errorHandler';
import { globalRateLimiter } from './middlewares/rateLimiter';
import routes from './routes';
import config from './config';
import { logger } from './utils/logger';

/**
 * Create and configure Express application
 * Separated from index.ts for testing purposes
 */
export function createApp(): Application {
  const app = express();

  // Security
  app.use(helmet());

  // CORS
  app.use(cors({
    origin: process.env.CORS_ORIGIN || '*',
    credentials: true,
  }));

  // Compression
  app.use(compression());

  // Body parsing
  app.use(express.json({ limit: '10mb' }));
  app.use(express.urlencoded({ extended: true, limit: '10mb' }));

  // Global rate limiting (skip in test environment)
  if (process.env.NODE_ENV !== 'test') {
    app.use(globalRateLimiter);
  }

  // Request logging (skip in test environment)
  if (process.env.NODE_ENV !== 'test') {
    app.use((req, res, next) => {
      logger.info(`${req.method} ${req.path}`, {
        ip: req.ip,
        userAgent: req.get('user-agent'),
      });
      next();
    });
  }

  // API routes
  app.use(`/api/${config.apiVersion}`, routes);

  // 404 handler
  app.use((req, res) => {
    res.status(404).json({
      success: false,
      error: {
        code: 'NOT_FOUND',
        message: 'Route not found',
      },
    });
  });

  // Error handling
  app.use(errorHandler);

  return app;
}

// Export app instance for testing
export const app = createApp();
