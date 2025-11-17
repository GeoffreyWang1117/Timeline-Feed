import dotenv from 'dotenv';

dotenv.config();

export const config = {
  // Application
  nodeEnv: process.env.NODE_ENV || 'development',
  port: Number(process.env.PORT) || 3000,
  apiVersion: process.env.API_VERSION || 'v1',

  // Database
  mongodb: {
    uri: process.env.MONGODB_URI || 'mongodb://localhost:27017/timeline_feed',
    maxPoolSize: Number(process.env.MONGODB_MAX_POOL_SIZE) || 10,
  },

  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD,
    db: Number(process.env.REDIS_DB) || 0,
  },

  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    clientId: process.env.KAFKA_CLIENT_ID || 'timeline-feed-service',
    groupId: process.env.KAFKA_GROUP_ID || 'timeline-feed-group',
  },

  // Rate Limiting
  rateLimit: {
    windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS) || 60000, // 1 minute
    maxRequests: Number(process.env.RATE_LIMIT_MAX_REQUESTS) || 100,
  },

  // Cache
  cache: {
    ttl: Number(process.env.CACHE_TTL) || 1800,
    timelineCacheSize: Number(process.env.TIMELINE_CACHE_SIZE) || 1000,
    hotContentThreshold: Number(process.env.HOT_CONTENT_THRESHOLD) || 1000,
  },

  // Fanout
  fanout: {
    celebrityThreshold: Number(process.env.FANOUT_CELEBRITY_THRESHOLD) || 10000,
    batchSize: Number(process.env.FANOUT_BATCH_SIZE) || 1000,
  },

  // JWT
  jwt: {
    secret: process.env.JWT_SECRET || 'your-secret-key-change-in-production',
    expiresIn: process.env.JWT_EXPIRES_IN || '7d',
  },

  // Logging
  log: {
    level: process.env.LOG_LEVEL || 'info',
  },
} as const;

export default config;
