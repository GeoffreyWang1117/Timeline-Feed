import { redis, REDIS_KEYS, CACHE_TTL } from '../config/database';
import { logger } from '../utils/logger';
import config from '../config';
import { TimelinePost, HotContentMetrics } from '../types';

export class CacheService {
  /**
   * Add post to user's timeline cache (Redis Sorted Set)
   */
  async addToTimeline(userId: string, postId: string, timestamp: number): Promise<void> {
    try {
      const key = REDIS_KEYS.TIMELINE(userId);

      // Add to sorted set (score = timestamp)
      await redis.zadd(key, timestamp, postId);

      // Trim to max size
      const maxSize = config.cache.timelineCacheSize;
      await redis.zremrangebyrank(key, 0, -(maxSize + 1));

      // Set TTL
      await redis.expire(key, CACHE_TTL.TIMELINE);

      logger.debug(`Added post ${postId} to timeline of user ${userId}`);
    } catch (error) {
      logger.error('Error adding to timeline:', error);
      throw error;
    }
  }

  /**
   * Add multiple posts to timeline (batch operation)
   */
  async addMultipleToTimeline(
    userId: string,
    posts: Array<{ postId: string; timestamp: number }>
  ): Promise<void> {
    if (posts.length === 0) return;

    try {
      const key = REDIS_KEYS.TIMELINE(userId);
      const pipeline = redis.pipeline();

      // Batch zadd
      posts.forEach(({ postId, timestamp }) => {
        pipeline.zadd(key, timestamp, postId);
      });

      // Trim to max size
      const maxSize = config.cache.timelineCacheSize;
      pipeline.zremrangebyrank(key, 0, -(maxSize + 1));

      // Set TTL
      pipeline.expire(key, CACHE_TTL.TIMELINE);

      await pipeline.exec();

      logger.debug(`Added ${posts.length} posts to timeline of user ${userId}`);
    } catch (error) {
      logger.error('Error adding multiple to timeline:', error);
      throw error;
    }
  }

  /**
   * Get timeline posts (with pagination)
   */
  async getTimeline(
    userId: string,
    limit: number,
    cursor?: string
  ): Promise<{ postIds: string[]; nextCursor?: string }> {
    try {
      const key = REDIS_KEYS.TIMELINE(userId);

      let start = 0;
      let maxScore = '+inf';

      // Parse cursor
      if (cursor) {
        const cursorData = this.decodeCursor(cursor);
        maxScore = cursorData.timestamp.toString();
      }

      // Get posts from sorted set (reverse order - newest first)
      const postIds = await redis.zrevrangebyscore(
        key,
        maxScore,
        '-inf',
        'LIMIT',
        start,
        limit + 1 // Fetch one extra to check if there are more
      );

      const hasMore = postIds.length > limit;
      const results = hasMore ? postIds.slice(0, limit) : postIds;

      // Generate next cursor
      let nextCursor: string | undefined;
      if (hasMore) {
        const lastPostId = results[results.length - 1];
        const lastScore = await redis.zscore(key, lastPostId);
        if (lastScore) {
          nextCursor = this.encodeCursor({
            timestamp: parseInt(lastScore),
            postId: lastPostId,
          });
        }
      }

      return { postIds: results, nextCursor };
    } catch (error) {
      logger.error('Error getting timeline:', error);
      throw error;
    }
  }

  /**
   * Cache post data
   */
  async cachePost(postId: string, postData: any): Promise<void> {
    try {
      const key = REDIS_KEYS.POST(postId);
      await redis.setex(key, CACHE_TTL.POST, JSON.stringify(postData));
      logger.debug(`Cached post ${postId}`);
    } catch (error) {
      logger.error('Error caching post:', error);
      throw error;
    }
  }

  /**
   * Get cached post
   */
  async getCachedPost(postId: string): Promise<any | null> {
    try {
      const key = REDIS_KEYS.POST(postId);
      const data = await redis.get(key);
      return data ? JSON.parse(data) : null;
    } catch (error) {
      logger.error('Error getting cached post:', error);
      return null;
    }
  }

  /**
   * Cache multiple posts (batch)
   */
  async cacheMultiplePosts(posts: Array<{ postId: string; data: any }>): Promise<void> {
    if (posts.length === 0) return;

    try {
      const pipeline = redis.pipeline();

      posts.forEach(({ postId, data }) => {
        const key = REDIS_KEYS.POST(postId);
        pipeline.setex(key, CACHE_TTL.POST, JSON.stringify(data));
      });

      await pipeline.exec();
      logger.debug(`Cached ${posts.length} posts`);
    } catch (error) {
      logger.error('Error caching multiple posts:', error);
      throw error;
    }
  }

  /**
   * Increment post views
   */
  async incrementPostViews(postId: string): Promise<number> {
    try {
      const key = REDIS_KEYS.POST_VIEWS(postId);
      const views = await redis.incr(key);
      await redis.expire(key, 86400); // 24 hours
      return views;
    } catch (error) {
      logger.error('Error incrementing post views:', error);
      throw error;
    }
  }

  /**
   * Track unique visitor (for hot content detection)
   */
  async trackVisitor(postId: string, userId: string): Promise<void> {
    try {
      const key = REDIS_KEYS.HOT_CONTENT_VISITORS(postId);
      await redis.pfadd(key, userId);
      await redis.expire(key, CACHE_TTL.HOT_CONTENT);
    } catch (error) {
      logger.error('Error tracking visitor:', error);
      throw error;
    }
  }

  /**
   * Get unique visitor count (HyperLogLog)
   */
  async getUniqueVisitorCount(postId: string): Promise<number> {
    try {
      const key = REDIS_KEYS.HOT_CONTENT_VISITORS(postId);
      return await redis.pfcount(key);
    } catch (error) {
      logger.error('Error getting unique visitor count:', error);
      return 0;
    }
  }

  /**
   * Add to trending posts
   */
  async addToTrending(postId: string, score: number): Promise<void> {
    try {
      const key = REDIS_KEYS.TRENDING_POSTS;
      await redis.zincrby(key, score, postId);

      // Keep only top 1000
      await redis.zremrangebyrank(key, 0, -1001);
    } catch (error) {
      logger.error('Error adding to trending:', error);
      throw error;
    }
  }

  /**
   * Get trending posts
   */
  async getTrendingPosts(limit = 100): Promise<string[]> {
    try {
      const key = REDIS_KEYS.TRENDING_POSTS;
      return await redis.zrevrange(key, 0, limit - 1);
    } catch (error) {
      logger.error('Error getting trending posts:', error);
      return [];
    }
  }

  /**
   * Check if content is hot
   */
  async isHotContent(postId: string): Promise<boolean> {
    try {
      const views = await redis.get(REDIS_KEYS.POST_VIEWS(postId));
      const viewCount = views ? parseInt(views) : 0;
      return viewCount >= config.cache.hotContentThreshold;
    } catch (error) {
      logger.error('Error checking hot content:', error);
      return false;
    }
  }

  /**
   * Clear timeline cache
   */
  async clearTimeline(userId: string): Promise<void> {
    try {
      await redis.del(REDIS_KEYS.TIMELINE(userId));
      logger.debug(`Cleared timeline for user ${userId}`);
    } catch (error) {
      logger.error('Error clearing timeline:', error);
      throw error;
    }
  }

  /**
   * Encode cursor (Base64)
   */
  private encodeCursor(data: { timestamp: number; postId: string }): string {
    return Buffer.from(JSON.stringify(data)).toString('base64');
  }

  /**
   * Decode cursor
   */
  private decodeCursor(cursor: string): { timestamp: number; postId: string } {
    try {
      const decoded = Buffer.from(cursor, 'base64').toString('utf-8');
      return JSON.parse(decoded);
    } catch (error) {
      throw new Error('Invalid cursor');
    }
  }
}

export const cacheService = new CacheService();
