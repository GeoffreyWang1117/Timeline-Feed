import { Request, Response, NextFunction } from 'express';
import { redis, REDIS_KEYS } from '../config/database';
import { logger } from '../utils/logger';
import config from '../config';

interface RateLimitRule {
  windowMs: number;
  maxRequests: number;
}

const DEFAULT_RULE: RateLimitRule = {
  windowMs: config.rateLimit.windowMs,
  maxRequests: config.rateLimit.maxRequests,
};

const RATE_LIMIT_RULES: Record<string, RateLimitRule> = {
  'POST:/api/v1/posts': { windowMs: 60000, maxRequests: 20 }, // 20 posts per minute
  'GET:/api/v1/timeline': { windowMs: 60000, maxRequests: 100 }, // 100 requests per minute
  'POST:/api/v1/users/*/follow': { windowMs: 60000, maxRequests: 50 }, // 50 follows per minute
};

/**
 * Rate limiter middleware using Redis + Lua script
 */
export const rateLimiter = (customRule?: RateLimitRule) => {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      // Get user ID from request (assumes auth middleware sets req.user)
      const userId = (req as any).user?.userId || req.ip || 'anonymous';

      // Determine rate limit rule
      const routeKey = `${req.method}:${req.route?.path || req.path}`;
      const rule = customRule || RATE_LIMIT_RULES[routeKey] || DEFAULT_RULE;

      // Create unique key
      const key = REDIS_KEYS.RATE_LIMIT(`${userId}:${routeKey}`);
      const windowSeconds = Math.ceil(rule.windowMs / 1000);

      // Lua script for atomic rate limiting (Token Bucket)
      const luaScript = `
        local key = KEYS[1]
        local limit = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local current_time = tonumber(ARGV[3])

        local current = redis.call('GET', key)

        if current == false then
          redis.call('SET', key, 1, 'EX', window)
          return { 1, limit - 1, current_time + window }
        else
          current = tonumber(current)
          if current < limit then
            redis.call('INCR', key)
            local ttl = redis.call('TTL', key)
            return { current + 1, limit - current - 1, current_time + ttl }
          else
            local ttl = redis.call('TTL', key)
            return { current, 0, current_time + ttl }
          end
        end
      `;

      const currentTime = Math.floor(Date.now() / 1000);
      const result: any = await redis.eval(
        luaScript,
        1,
        key,
        rule.maxRequests.toString(),
        windowSeconds.toString(),
        currentTime.toString()
      );

      const [currentCount, remaining, resetTime] = result as [number, number, number];

      // Set rate limit headers
      res.setHeader('X-RateLimit-Limit', rule.maxRequests);
      res.setHeader('X-RateLimit-Remaining', Math.max(0, remaining));
      res.setHeader('X-RateLimit-Reset', resetTime);

      if (currentCount > rule.maxRequests) {
        const retryAfter = resetTime - currentTime;
        res.setHeader('Retry-After', retryAfter);

        logger.warn(`Rate limit exceeded for ${userId} on ${routeKey}`);

        res.status(429).json({
          success: false,
          error: {
            code: 'RATE_LIMIT_EXCEEDED',
            message: 'Too many requests. Please try again later.',
          },
          retryAfter,
        });
        return;
      }

      next();
    } catch (error) {
      logger.error('Rate limiter error:', error);
      // Fail open - allow request if rate limiter fails
      next();
    }
  };
};

/**
 * Global rate limiter (by IP)
 */
export const globalRateLimiter = rateLimiter({
  windowMs: 60000, // 1 minute
  maxRequests: 1000, // 1000 requests per minute per IP
});

/**
 * Strict rate limiter for sensitive operations
 */
export const strictRateLimiter = rateLimiter({
  windowMs: 60000, // 1 minute
  maxRequests: 10, // 10 requests per minute
});
