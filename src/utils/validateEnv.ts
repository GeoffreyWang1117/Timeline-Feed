import { logger } from './logger';
import config from '../config';

/**
 * Validate required environment variables
 * Prevents application startup with missing or invalid configuration
 */
export function validateEnvironment(): void {
  const errors: string[] = [];

  // Required environment variables
  const requiredVars = [
    'MONGODB_URI',
    'REDIS_HOST',
    'KAFKA_BROKERS',
    'JWT_SECRET',
  ];

  // Check if variables are set
  requiredVars.forEach((varName) => {
    if (!process.env[varName]) {
      errors.push(`Missing required environment variable: ${varName}`);
    }
  });

  // Production-specific validations
  if (config.nodeEnv === 'production') {
    // JWT secret must not be the default value
    if (config.jwt.secret === 'your-secret-key-change-in-production') {
      errors.push(
        'JWT_SECRET must be changed from default value in production'
      );
    }

    // JWT secret must be strong enough
    if (config.jwt.secret.length < 32) {
      errors.push(
        'JWT_SECRET must be at least 32 characters long in production'
      );
    }

    // MongoDB URI should use authentication
    if (
      config.mongodb.uri.includes('localhost') ||
      !config.mongodb.uri.includes('@')
    ) {
      errors.push(
        'MongoDB URI should use authentication and not localhost in production'
      );
    }

    // Redis should have password in production
    if (!config.redis.password) {
      logger.warn(
        'Redis password not set in production - this is a security risk'
      );
    }
  }

  // Validate numeric values
  if (isNaN(config.port) || config.port < 1 || config.port > 65535) {
    errors.push(`Invalid PORT value: ${process.env.PORT}`);
  }

  if (
    isNaN(config.redis.port) ||
    config.redis.port < 1 ||
    config.redis.port > 65535
  ) {
    errors.push(`Invalid REDIS_PORT value: ${process.env.REDIS_PORT}`);
  }

  // Validate cache configuration
  if (
    config.cache.timelineCacheSize < 10 ||
    config.cache.timelineCacheSize > 10000
  ) {
    errors.push(
      `TIMELINE_CACHE_SIZE must be between 10 and 10000, got: ${config.cache.timelineCacheSize}`
    );
  }

  if (
    config.fanout.celebrityThreshold < 100 ||
    config.fanout.celebrityThreshold > 1000000
  ) {
    errors.push(
      `FANOUT_CELEBRITY_THRESHOLD must be between 100 and 1000000, got: ${config.fanout.celebrityThreshold}`
    );
  }

  // If there are errors, log them and exit
  if (errors.length > 0) {
    logger.error('Environment validation failed:');
    errors.forEach((error) => logger.error(`  - ${error}`));
    logger.error('\nPlease fix the above errors and restart the application.');
    process.exit(1);
  }

  // Log success
  logger.info('Environment validation passed');
  logger.info(`Running in ${config.nodeEnv} mode`);
}

/**
 * Validate that sensitive data is not exposed in logs
 */
export function sanitizeConfig(conf: any): any {
  const sanitized = { ...conf };

  // Remove sensitive fields
  if (sanitized.jwt) {
    sanitized.jwt = { ...sanitized.jwt, secret: '***REDACTED***' };
  }

  if (sanitized.mongodb) {
    sanitized.mongodb = {
      ...sanitized.mongodb,
      uri: sanitized.mongodb.uri.replace(/:([^@]+)@/, ':***REDACTED***@'),
    };
  }

  if (sanitized.redis && sanitized.redis.password) {
    sanitized.redis = { ...sanitized.redis, password: '***REDACTED***' };
  }

  return sanitized;
}
