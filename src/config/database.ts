import mongoose from 'mongoose';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';
import { logger } from '../utils/logger';

// MongoDB Configuration
export const connectMongoDB = async (): Promise<void> => {
  try {
    const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017/timeline_feed';

    await mongoose.connect(uri, {
      maxPoolSize: Number(process.env.MONGODB_MAX_POOL_SIZE) || 10,
    });

    logger.info('MongoDB connected successfully');

    mongoose.connection.on('error', (error) => {
      logger.error('MongoDB connection error:', error);
    });

    mongoose.connection.on('disconnected', () => {
      logger.warn('MongoDB disconnected');
    });
  } catch (error) {
    logger.error('Failed to connect to MongoDB:', error);
    process.exit(1);
  }
};

// Redis Configuration
export const createRedisClient = (): Redis => {
  const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    db: Number(process.env.REDIS_DB) || 0,
    retryStrategy: (times: number) => {
      const delay = Math.min(times * 50, 2000);
      return delay;
    },
    maxRetriesPerRequest: 3,
  });

  redis.on('connect', () => {
    logger.info('Redis connected successfully');
  });

  redis.on('error', (error) => {
    logger.error('Redis connection error:', error);
  });

  redis.on('close', () => {
    logger.warn('Redis connection closed');
  });

  return redis;
};

// Kafka Configuration
export const createKafkaClient = (): Kafka => {
  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'timeline-feed-service',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    retry: {
      initialRetryTime: 300,
      retries: 8,
    },
  });

  logger.info('Kafka client created');

  return kafka;
};

// Kafka Topics
export const KAFKA_TOPICS = {
  NEW_POST: 'new-post',
  FANOUT_COMPLETE: 'fanout-complete',
  HOT_CONTENT: 'hot-content',
} as const;

// Redis Keys
export const REDIS_KEYS = {
  TIMELINE: (userId: string) => `timeline:${userId}`,
  FOLLOWERS: (userId: string) => `followers:${userId}`,
  FOLLOWING: (userId: string) => `following:${userId}`,
  POST: (postId: string) => `post:${postId}`,
  POST_VIEWS: (postId: string) => `post:${postId}:views`,
  USER_POSTS: (userId: string) => `user:${userId}:posts`,
  TRENDING_POSTS: 'trending:posts',
  RATE_LIMIT: (key: string) => `ratelimit:${key}`,
  HOT_CONTENT_VISITORS: (postId: string) => `post:${postId}:visitors`,
} as const;

// Cache TTL (in seconds)
export const CACHE_TTL = {
  TIMELINE: 1800, // 30 minutes
  POST: 3600, // 1 hour
  USER_POSTS: 600, // 10 minutes
  FOLLOWERS: 3600, // 1 hour
  HOT_CONTENT: 7200, // 2 hours
} as const;

export const redis = createRedisClient();
export const kafka = createKafkaClient();
