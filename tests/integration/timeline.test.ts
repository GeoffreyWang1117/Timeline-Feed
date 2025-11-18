import request from 'supertest';
import { app } from '../../src/app';
import { User } from '../../src/models/User';
import { Post } from '../../src/models/Post';
import { Follow } from '../../src/models/Follow';
import jwt from 'jsonwebtoken';
import config from '../../src/config';
import { v4 as uuidv4 } from 'uuid';

// Mock external services
jest.mock('../../src/services/KafkaService', () => ({
  kafkaService: {
    publishNewPost: jest.fn().mockResolvedValue(undefined),
    publishFanoutComplete: jest.fn().mockResolvedValue(undefined),
    publishHotContent: jest.fn().mockResolvedValue(undefined),
    initProducer: jest.fn().mockResolvedValue(undefined),
    initConsumer: jest.fn().mockResolvedValue(undefined),
  },
}));

jest.mock('../../src/config/database', () => {
  const mockRedis = {
    zadd: jest.fn().mockResolvedValue(1),
    zremrangebyrank: jest.fn().mockResolvedValue(0),
    expire: jest.fn().mockResolvedValue(1),
    zrevrangebyscore: jest.fn().mockResolvedValue([]),
    zscore: jest.fn().mockResolvedValue(null),
    get: jest.fn().mockResolvedValue(null),
    mget: jest.fn().mockResolvedValue([]),
    setex: jest.fn().mockResolvedValue('OK'),
    pipeline: jest.fn(() => ({
      zadd: jest.fn(),
      zremrangebyrank: jest.fn(),
      expire: jest.fn(),
      setex: jest.fn(),
      exec: jest.fn().mockResolvedValue([]),
    })),
    incr: jest.fn().mockResolvedValue(1),
    pfadd: jest.fn().mockResolvedValue(1),
    pfcount: jest.fn().mockResolvedValue(0),
    zincrby: jest.fn().mockResolvedValue(1),
    zrevrange: jest.fn().mockResolvedValue([]),
    del: jest.fn().mockResolvedValue(1),
    smembers: jest.fn().mockResolvedValue([]),
    sadd: jest.fn().mockResolvedValue(1),
    srem: jest.fn().mockResolvedValue(1),
    sismember: jest.fn().mockResolvedValue(0),
  };

  return {
    redis: mockRedis,
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
    KAFKA_TOPICS: {
      NEW_POST: 'new-post',
      FANOUT_COMPLETE: 'fanout-complete',
      HOT_CONTENT: 'hot-content',
    },
    kafka: {
      producer: jest.fn(),
      consumer: jest.fn(),
    },
  };
});

describe('Timeline API Integration Tests', () => {
  let authToken: string;
  let testUser: any;
  let testPost: any;

  beforeAll(async () => {
    // Create test user
    const userId = uuidv4();
    testUser = await User.create({
      userId,
      username: 'testuser',
      email: 'test@example.com',
      displayName: 'Test User',
      followerCount: 0,
      followingCount: 0,
      postCount: 0,
    });

    // Generate JWT token
    authToken = jwt.sign(
      { userId: testUser.userId, username: testUser.username },
      config.jwt.secret,
      { expiresIn: config.jwt.expiresIn }
    );
  });

  afterAll(async () => {
    // Cleanup
    await User.deleteMany({});
    await Post.deleteMany({});
    await Follow.deleteMany({});
  });

  describe('POST /api/v1/posts - Create Post', () => {
    it('should create a new post with valid authentication', async () => {
      const response = await request(app)
        .post('/api/v1/posts')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          content: 'This is a test post!',
        })
        .expect(201);

      expect(response.body.success).toBe(true);
      expect(response.body.data.post).toHaveProperty('postId');
      expect(response.body.data.post.content).toBe('This is a test post!');
      expect(response.body.data.post.userId).toBe(testUser.userId);

      testPost = response.body.data.post;
    });

    it('should reject post without authentication', async () => {
      const response = await request(app)
        .post('/api/v1/posts')
        .send({
          content: 'Unauthorized post',
        })
        .expect(401);

      expect(response.body.success).toBe(false);
    });

    it('should validate content length (max 280 characters)', async () => {
      const longContent = 'a'.repeat(281);

      const response = await request(app)
        .post('/api/v1/posts')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          content: longContent,
        })
        .expect(400);

      expect(response.body.success).toBe(false);
      expect(response.body.error.code).toBe('CONTENT_TOO_LONG');
    });

    it('should escape XSS attempts in content', async () => {
      const xssContent = '<script>alert("XSS")</script>';

      const response = await request(app)
        .post('/api/v1/posts')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          content: xssContent,
        })
        .expect(201);

      // Content should be escaped
      expect(response.body.data.post.content).toContain('&lt;script&gt;');
      expect(response.body.data.post.content).not.toContain('<script>');
    });

    it('should validate media URLs (HTTPS only)', async () => {
      const response = await request(app)
        .post('/api/v1/posts')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          content: 'Post with media',
          mediaUrls: ['http://example.com/image.jpg'], // HTTP not allowed
        })
        .expect(400);

      expect(response.body.success).toBe(false);
      expect(response.body.error.message).toContain('HTTPS');
    });

    it('should reject more than 4 media URLs', async () => {
      const response = await request(app)
        .post('/api/v1/posts')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          content: 'Too many images',
          mediaUrls: [
            'https://imgur.com/1.jpg',
            'https://imgur.com/2.jpg',
            'https://imgur.com/3.jpg',
            'https://imgur.com/4.jpg',
            'https://imgur.com/5.jpg',
          ],
        })
        .expect(400);

      expect(response.body.error.code).toBe('TOO_MANY_MEDIA');
    });
  });

  describe('GET /api/v1/posts/:postId - Get Post', () => {
    it('should retrieve post with valid UUID', async () => {
      const response = await request(app)
        .get(`/api/v1/posts/${testPost.postId}`)
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data.post.postId).toBe(testPost.postId);
    });

    it('should reject invalid UUID format', async () => {
      const response = await request(app)
        .get('/api/v1/posts/invalid-uuid')
        .expect(400);

      expect(response.body.success).toBe(false);
      expect(response.body.error.code).toBe('INVALID_ID');
    });

    it('should return 404 for non-existent post', async () => {
      const nonExistentId = uuidv4();

      const response = await request(app)
        .get(`/api/v1/posts/${nonExistentId}`)
        .expect(404);

      expect(response.body.success).toBe(false);
    });
  });

  describe('POST /api/v1/posts/:postId/like - Like Post', () => {
    it('should like a post with authentication', async () => {
      const response = await request(app)
        .post(`/api/v1/posts/${testPost.postId}/like`)
        .set('Authorization', `Bearer ${authToken}`)
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.message).toContain('liked');
    });

    it('should reject like without authentication', async () => {
      const response = await request(app)
        .post(`/api/v1/posts/${testPost.postId}/like`)
        .expect(401);

      expect(response.body.success).toBe(false);
    });

    it('should validate UUID in like endpoint', async () => {
      const response = await request(app)
        .post('/api/v1/posts/malicious-input/like')
        .set('Authorization', `Bearer ${authToken}`)
        .expect(400);

      expect(response.body.error.code).toBe('INVALID_ID');
    });
  });

  describe('GET /api/v1/timeline - Get Timeline', () => {
    it('should retrieve timeline with authentication', async () => {
      const response = await request(app)
        .get('/api/v1/timeline')
        .set('Authorization', `Bearer ${authToken}`)
        .query({ limit: 20 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data).toHaveProperty('posts');
      expect(Array.isArray(response.body.data.posts)).toBe(true);
    });

    it('should reject timeline request without authentication', async () => {
      const response = await request(app)
        .get('/api/v1/timeline')
        .expect(401);

      expect(response.body.success).toBe(false);
    });

    it('should validate cursor format', async () => {
      const response = await request(app)
        .get('/api/v1/timeline')
        .set('Authorization', `Bearer ${authToken}`)
        .query({ cursor: 'invalid-cursor!!!' })
        .expect(400);

      expect(response.body.error.code).toBe('INVALID_CURSOR');
    });

    it('should accept valid base64 cursor', async () => {
      const validCursor = Buffer.from(
        JSON.stringify({ timestamp: 123456, postId: 'test' })
      ).toString('base64');

      const response = await request(app)
        .get('/api/v1/timeline')
        .set('Authorization', `Bearer ${authToken}`)
        .query({ cursor: validCursor })
        .expect(200);

      expect(response.body.success).toBe(true);
    });
  });

  describe('GET /api/v1/timeline/trending - Get Trending', () => {
    it('should retrieve trending posts without authentication', async () => {
      const response = await request(app)
        .get('/api/v1/timeline/trending')
        .query({ limit: 10 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data).toHaveProperty('posts');
    });

    it('should validate pagination parameters', async () => {
      const response = await request(app)
        .get('/api/v1/timeline/trending')
        .query({ limit: 150 }) // Over max
        .expect(400);

      expect(response.body.error.message).toContain('must be at most 100');
    });
  });

  describe('Rate Limiting', () => {
    it('should apply rate limiting to sensitive endpoints', async () => {
      // Make multiple rapid requests
      const requests = Array(10).fill(null).map(() =>
        request(app)
          .post('/api/v1/timeline/refresh')
          .set('Authorization', `Bearer ${authToken}`)
      );

      const responses = await Promise.all(requests);

      // Some requests should be rate limited
      const rateLimited = responses.filter(r => r.status === 429);
      expect(rateLimited.length).toBeGreaterThan(0);
    }, 15000);
  });

  describe('Follow System Integration', () => {
    let targetUser: any;

    beforeAll(async () => {
      const userId = uuidv4();
      targetUser = await User.create({
        userId,
        username: 'targetuser',
        email: 'target@example.com',
        displayName: 'Target User',
        followerCount: 0,
        followingCount: 0,
        postCount: 0,
      });
    });

    it('should follow user with valid UUID', async () => {
      const response = await request(app)
        .post(`/api/v1/users/${targetUser.userId}/follow`)
        .set('Authorization', `Bearer ${authToken}`)
        .expect(200);

      expect(response.body.success).toBe(true);
    });

    it('should reject follow with invalid UUID', async () => {
      const response = await request(app)
        .post('/api/v1/users/invalid-uuid/follow')
        .set('Authorization', `Bearer ${authToken}`)
        .expect(400);

      expect(response.body.error.code).toBe('INVALID_ID');
    });

    it('should get followers with pagination validation', async () => {
      const response = await request(app)
        .get(`/api/v1/users/${targetUser.userId}/followers`)
        .query({ limit: 50, offset: 0 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data).toHaveProperty('followers');
    });
  });

  describe('Security - Attack Prevention', () => {
    it('should prevent SQL injection in UUID parameters', async () => {
      const sqlInjection = "'; DROP TABLE posts; --";

      const response = await request(app)
        .get(`/api/v1/posts/${sqlInjection}`)
        .expect(400);

      expect(response.body.error.code).toBe('INVALID_ID');
    });

    it('should prevent NoSQL injection in request body', async () => {
      const response = await request(app)
        .post('/api/v1/posts')
        .set('Authorization', `Bearer ${authToken}`)
        .send({
          content: { $gt: '' }, // NoSQL injection attempt
        })
        .expect(400);

      expect(response.body.success).toBe(false);
    });
  });
});
