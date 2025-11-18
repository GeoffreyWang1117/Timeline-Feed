import { CacheService } from '../CacheService';
import { redis, REDIS_KEYS } from '../../config/database';

// Mock Redis and dependencies
jest.mock('../../config/database', () => ({
  redis: {
    zadd: jest.fn(),
    zremrangebyrank: jest.fn(),
    expire: jest.fn(),
    zrevrangebyscore: jest.fn(),
    zscore: jest.fn(),
    get: jest.fn(),
    mget: jest.fn(),
    setex: jest.fn(),
    pipeline: jest.fn(),
    incr: jest.fn(),
    pfadd: jest.fn(),
    pfcount: jest.fn(),
    zincrby: jest.fn(),
    zrevrange: jest.fn(),
    del: jest.fn(),
    smembers: jest.fn(),
    sadd: jest.fn(),
    srem: jest.fn(),
    sismember: jest.fn(),
  },
  REDIS_KEYS: {
    TIMELINE: (userId: string) => `timeline:${userId}`,
    POST: (postId: string) => `post:${postId}`,
    POST_VIEWS: (postId: string) => `post:views:${postId}`,
    HOT_CONTENT_VISITORS: (postId: string) => `hot:visitors:${postId}`,
    TRENDING_POSTS: 'trending:posts',
    FOLLOWERS: (userId: string) => `followers:${userId}`,
    FOLLOWING: (userId: string) => `following:${userId}`,
  },
  CACHE_TTL: {
    TIMELINE: 3600,
    POST: 1800,
    HOT_CONTENT: 86400,
    FOLLOWERS: 3600,
  },
}));

jest.mock('../../config', () => ({
  default: {
    cache: {
      timelineCacheSize: 1000,
      hotContentThreshold: 1000,
    },
    jwt: {
      secret: 'test-secret-key-for-testing-purposes-only',
    },
  },
}));

describe('CacheService', () => {
  let cacheService: CacheService;

  beforeEach(() => {
    // Create new instance for each test
    cacheService = new CacheService();
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('addToTimeline', () => {
    it('should add post to user timeline with circuit breaker', async () => {
      const userId = 'user-123';
      const postId = 'post-456';
      const timestamp = Date.now();

      (redis.zadd as jest.Mock).mockResolvedValue(1);
      (redis.zremrangebyrank as jest.Mock).mockResolvedValue(0);
      (redis.expire as jest.Mock).mockResolvedValue(1);

      await cacheService.addToTimeline(userId, postId, timestamp);

      expect(redis.zadd).toHaveBeenCalledWith(
        `timeline:${userId}`,
        timestamp,
        postId
      );
      expect(redis.zremrangebyrank).toHaveBeenCalled();
      expect(redis.expire).toHaveBeenCalled();
    });

    it('should handle Redis errors gracefully without throwing', async () => {
      const userId = 'user-123';
      const postId = 'post-456';
      const timestamp = Date.now();

      (redis.zadd as jest.Mock).mockRejectedValue(new Error('Redis connection failed'));

      // Should not throw
      await expect(
        cacheService.addToTimeline(userId, postId, timestamp)
      ).resolves.not.toThrow();
    });
  });

  describe('getTimeline', () => {
    it('should retrieve timeline posts successfully', async () => {
      const userId = 'user-123';
      const limit = 20;
      const mockPostIds = ['post-1', 'post-2', 'post-3'];

      (redis.zrevrangebyscore as jest.Mock).mockResolvedValue(mockPostIds);

      const result = await cacheService.getTimeline(userId, limit);

      expect(result.postIds).toEqual(mockPostIds);
      expect(result.nextCursor).toBeUndefined();
      expect(redis.zrevrangebyscore).toHaveBeenCalledWith(
        `timeline:${userId}`,
        '+inf',
        '-inf',
        'LIMIT',
        0,
        limit + 1
      );
    });

    it('should generate next cursor when there are more posts', async () => {
      const userId = 'user-123';
      const limit = 2;
      const mockPostIds = ['post-1', 'post-2', 'post-3']; // More than limit

      (redis.zrevrangebyscore as jest.Mock).mockResolvedValue(mockPostIds);
      (redis.zscore as jest.Mock).mockResolvedValue('1234567890');

      const result = await cacheService.getTimeline(userId, limit);

      expect(result.postIds).toHaveLength(2);
      expect(result.nextCursor).toBeDefined();
      expect(redis.zscore).toHaveBeenCalledWith(`timeline:${userId}`, 'post-2');
    });

    it('should handle Redis failure with circuit breaker fallback', async () => {
      const userId = 'user-123';
      const limit = 20;

      (redis.zrevrangebyscore as jest.Mock).mockRejectedValue(
        new Error('Redis connection timeout')
      );

      const result = await cacheService.getTimeline(userId, limit);

      // Should return empty result on failure
      expect(result.postIds).toEqual([]);
      expect(result.nextCursor).toBeUndefined();
    });
  });

  describe('getCachedPost', () => {
    it('should retrieve cached post successfully', async () => {
      const postId = 'post-123';
      const mockPost = { postId, content: 'Test post', userId: 'user-1' };

      (redis.get as jest.Mock).mockResolvedValue(JSON.stringify(mockPost));

      const result = await cacheService.getCachedPost(postId);

      expect(result).toEqual(mockPost);
      expect(redis.get).toHaveBeenCalledWith(`post:${postId}`);
    });

    it('should return null for cache miss', async () => {
      const postId = 'post-123';

      (redis.get as jest.Mock).mockResolvedValue(null);

      const result = await cacheService.getCachedPost(postId);

      expect(result).toBeNull();
    });

    it('should return null on Redis error (graceful degradation)', async () => {
      const postId = 'post-123';

      (redis.get as jest.Mock).mockRejectedValue(new Error('Connection lost'));

      const result = await cacheService.getCachedPost(postId);

      expect(result).toBeNull();
    });
  });

  describe('getCachedPosts - N+1 Query Fix', () => {
    it('should batch retrieve multiple posts using MGET', async () => {
      const postIds = ['post-1', 'post-2', 'post-3'];
      const mockPosts = [
        JSON.stringify({ postId: 'post-1', content: 'Post 1' }),
        JSON.stringify({ postId: 'post-2', content: 'Post 2' }),
        JSON.stringify({ postId: 'post-3', content: 'Post 3' }),
      ];

      (redis.mget as jest.Mock).mockResolvedValue(mockPosts);

      const result = await cacheService.getCachedPosts(postIds);

      expect(result.size).toBe(3);
      expect(result.get('post-1')).toEqual({ postId: 'post-1', content: 'Post 1' });
      expect(result.get('post-2')).toEqual({ postId: 'post-2', content: 'Post 2' });
      expect(result.get('post-3')).toEqual({ postId: 'post-3', content: 'Post 3' });

      // Verify single MGET call instead of N GET calls
      expect(redis.mget).toHaveBeenCalledTimes(1);
      expect(redis.mget).toHaveBeenCalledWith(
        'post:post-1',
        'post:post-2',
        'post:post-3'
      );
    });

    it('should handle partial cache hits', async () => {
      const postIds = ['post-1', 'post-2', 'post-3'];
      const mockValues = [
        JSON.stringify({ postId: 'post-1', content: 'Post 1' }),
        null, // Cache miss
        JSON.stringify({ postId: 'post-3', content: 'Post 3' }),
      ];

      (redis.mget as jest.Mock).mockResolvedValue(mockValues);

      const result = await cacheService.getCachedPosts(postIds);

      expect(result.size).toBe(2);
      expect(result.has('post-1')).toBe(true);
      expect(result.has('post-2')).toBe(false); // Cache miss
      expect(result.has('post-3')).toBe(true);
    });

    it('should return empty map for empty input', async () => {
      const result = await cacheService.getCachedPosts([]);

      expect(result.size).toBe(0);
      expect(redis.mget).not.toHaveBeenCalled();
    });

    it('should handle malformed JSON gracefully', async () => {
      const postIds = ['post-1', 'post-2'];
      const mockValues = [
        JSON.stringify({ postId: 'post-1', content: 'Post 1' }),
        'invalid-json{{{', // Malformed
      ];

      (redis.mget as jest.Mock).mockResolvedValue(mockValues);

      const result = await cacheService.getCachedPosts(postIds);

      expect(result.size).toBe(1); // Only valid post
      expect(result.has('post-1')).toBe(true);
      expect(result.has('post-2')).toBe(false);
    });

    it('should return empty map on Redis error', async () => {
      const postIds = ['post-1', 'post-2'];

      (redis.mget as jest.Mock).mockRejectedValue(new Error('Redis down'));

      const result = await cacheService.getCachedPosts(postIds);

      expect(result.size).toBe(0);
    });
  });

  describe('getTrendingPosts', () => {
    it('should retrieve trending posts with circuit breaker', async () => {
      const mockTrendingIds = ['post-1', 'post-2', 'post-3'];

      (redis.zrevrange as jest.Mock).mockResolvedValue(mockTrendingIds);

      const result = await cacheService.getTrendingPosts(100);

      expect(result).toEqual(mockTrendingIds);
      expect(redis.zrevrange).toHaveBeenCalledWith('trending:posts', 0, 99);
    });

    it('should return empty array on Redis failure', async () => {
      (redis.zrevrange as jest.Mock).mockRejectedValue(new Error('Redis error'));

      const result = await cacheService.getTrendingPosts(100);

      expect(result).toEqual([]);
    });
  });

  describe('encodeCursor and decodeCursor', () => {
    it('should encode and decode cursor with HMAC signature', async () => {
      const userId = 'user-123';
      const limit = 20;
      const mockPostIds = ['post-1', 'post-2', 'post-3'];

      (redis.zrevrangebyscore as jest.Mock).mockResolvedValue(mockPostIds);
      (redis.zscore as jest.Mock).mockResolvedValue('1234567890');

      // Get encoded cursor
      const result1 = await cacheService.getTimeline(userId, 2);
      const cursor = result1.nextCursor!;

      expect(cursor).toBeDefined();
      expect(cursor).toMatch(/^[A-Za-z0-9+/]+=*$/); // Base64 format

      // Use cursor for next page
      (redis.zrevrangebyscore as jest.Mock).mockResolvedValue(['post-3']);

      const result2 = await cacheService.getTimeline(userId, 20, cursor);

      expect(result2.postIds).toEqual(['post-3']);
      expect(redis.zrevrangebyscore).toHaveBeenCalledWith(
        `timeline:${userId}`,
        '1234567890',
        '-inf',
        'LIMIT',
        0,
        21
      );
    });
  });

  describe('Circuit Breaker Integration', () => {
    it('should protect multiple operations with same circuit breaker', async () => {
      // Simulate multiple operations
      (redis.get as jest.Mock).mockResolvedValue(null);
      (redis.mget as jest.Mock).mockResolvedValue([]);
      (redis.zrevrange as jest.Mock).mockResolvedValue([]);

      await cacheService.getCachedPost('post-1');
      await cacheService.getCachedPosts(['post-2', 'post-3']);
      await cacheService.getTrendingPosts(10);

      // All should use the same circuit breaker instance
      expect(redis.get).toHaveBeenCalledTimes(1);
      expect(redis.mget).toHaveBeenCalledTimes(1);
      expect(redis.zrevrange).toHaveBeenCalledTimes(1);
    });
  });
});
