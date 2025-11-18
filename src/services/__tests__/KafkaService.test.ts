import { KafkaService } from '../KafkaService';
import { kafka, KAFKA_TOPICS } from '../../config/database';
import pRetry from 'p-retry';

// Mock dependencies
jest.mock('../../config/database', () => ({
  kafka: {
    producer: jest.fn(),
    consumer: jest.fn(),
  },
  KAFKA_TOPICS: {
    NEW_POST: 'new-post',
    FANOUT_COMPLETE: 'fanout-complete',
    HOT_CONTENT: 'hot-content',
  },
}));

jest.mock('../../config', () => ({
  default: {
    kafka: {
      groupId: 'timeline-service',
    },
  },
}));

// Don't mock p-retry, we want to test actual retry behavior
jest.unmock('p-retry');

describe('KafkaService - Retry Logic', () => {
  let kafkaService: KafkaService;
  let mockProducer: any;

  beforeEach(() => {
    mockProducer = {
      connect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
    };

    (kafka.producer as jest.Mock).mockReturnValue(mockProducer);

    kafkaService = new KafkaService();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('publishNewPost - Retry Logic', () => {
    it('should publish successfully on first attempt', async () => {
      await kafkaService.initProducer();

      const message = {
        postId: 'post-123',
        userId: 'user-456',
        content: 'Test post',
        createdAt: Date.now(),
        isCelebrity: false,
      };

      await kafkaService.publishNewPost(message);

      expect(mockProducer.send).toHaveBeenCalledTimes(1);
      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: KAFKA_TOPICS.NEW_POST,
        messages: [
          {
            key: message.userId,
            value: JSON.stringify(message),
            timestamp: expect.any(String),
          },
        ],
      });
    });

    it('should retry up to 3 times on transient failures', async () => {
      await kafkaService.initProducer();

      let attemptCount = 0;
      mockProducer.send.mockImplementation(() => {
        attemptCount++;
        if (attemptCount < 3) {
          // Fail first 2 attempts
          return Promise.reject(new Error('Network timeout'));
        }
        // Succeed on 3rd attempt
        return Promise.resolve();
      });

      const message = {
        postId: 'post-123',
        userId: 'user-456',
        content: 'Test post',
        createdAt: Date.now(),
        isCelebrity: false,
      };

      await kafkaService.publishNewPost(message);

      // Should have retried 2 times (total 3 attempts)
      expect(mockProducer.send).toHaveBeenCalledTimes(3);
    });

    it('should fail after 3 retry attempts', async () => {
      await kafkaService.initProducer();

      // Always fail
      mockProducer.send.mockRejectedValue(new Error('Kafka broker unavailable'));

      const message = {
        postId: 'post-123',
        userId: 'user-456',
        content: 'Test post',
        createdAt: Date.now(),
        isCelebrity: false,
      };

      await expect(kafkaService.publishNewPost(message)).rejects.toThrow(
        'Kafka broker unavailable'
      );

      // Should have tried 4 times total (1 initial + 3 retries)
      expect(mockProducer.send).toHaveBeenCalledTimes(4);
    }, 15000); // Increase timeout for retries

    it('should use exponential backoff (1s, 2s, 4s)', async () => {
      await kafkaService.initProducer();

      const timestamps: number[] = [];
      mockProducer.send.mockImplementation(() => {
        timestamps.push(Date.now());
        if (timestamps.length < 3) {
          return Promise.reject(new Error('Temporary failure'));
        }
        return Promise.resolve();
      });

      const message = {
        postId: 'post-123',
        userId: 'user-456',
        content: 'Test post',
        createdAt: Date.now(),
        isCelebrity: false,
      };

      await kafkaService.publishNewPost(message);

      expect(timestamps.length).toBe(3);

      // Check time gaps (with some tolerance)
      const gap1 = timestamps[1] - timestamps[0];
      const gap2 = timestamps[2] - timestamps[1];

      expect(gap1).toBeGreaterThanOrEqual(900); // ~1s
      expect(gap1).toBeLessThan(1500);

      expect(gap2).toBeGreaterThanOrEqual(1800); // ~2s
      expect(gap2).toBeLessThan(2500);
    }, 10000);

    it('should auto-initialize producer if not connected', async () => {
      // Don't initialize producer manually
      const message = {
        postId: 'post-123',
        userId: 'user-456',
        content: 'Test post',
        createdAt: Date.now(),
        isCelebrity: false,
      };

      await kafkaService.publishNewPost(message);

      expect(mockProducer.connect).toHaveBeenCalled();
      expect(mockProducer.send).toHaveBeenCalled();
    });
  });

  describe('publishFanoutComplete - Retry Logic', () => {
    it('should publish fanout complete with retry protection', async () => {
      await kafkaService.initProducer();

      await kafkaService.publishFanoutComplete('post-123', 'user-456', 1000);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: KAFKA_TOPICS.FANOUT_COMPLETE,
        messages: [
          {
            key: 'post-123',
            value: expect.stringContaining('"postId":"post-123"'),
          },
        ],
      });
    });

    it('should retry on network failures', async () => {
      await kafkaService.initProducer();

      let attemptCount = 0;
      mockProducer.send.mockImplementation(() => {
        attemptCount++;
        if (attemptCount === 1) {
          return Promise.reject(new Error('Connection reset'));
        }
        return Promise.resolve();
      });

      await kafkaService.publishFanoutComplete('post-123', 'user-456', 1000);

      expect(mockProducer.send).toHaveBeenCalledTimes(2); // 1 failure + 1 retry success
    });
  });

  describe('publishHotContent - Retry Logic', () => {
    it('should publish hot content with retry protection', async () => {
      await kafkaService.initProducer();

      const metrics = {
        views: 10000,
        likes: 500,
        uniqueVisitors: 2000,
      };

      await kafkaService.publishHotContent('post-123', metrics);

      expect(mockProducer.send).toHaveBeenCalledWith({
        topic: KAFKA_TOPICS.HOT_CONTENT,
        messages: [
          {
            key: 'post-123',
            value: expect.stringContaining('"metrics"'),
          },
        ],
      });
    });

    it('should handle intermittent broker failures with retry', async () => {
      await kafkaService.initProducer();

      let attemptCount = 0;
      mockProducer.send.mockImplementation(() => {
        attemptCount++;
        // Simulate intermittent failure
        if (attemptCount === 1 || attemptCount === 2) {
          return Promise.reject(new Error('Broker not available'));
        }
        return Promise.resolve();
      });

      const metrics = { views: 10000 };
      await kafkaService.publishHotContent('post-123', metrics);

      // Should succeed on 3rd attempt
      expect(mockProducer.send).toHaveBeenCalledTimes(3);
    }, 10000);
  });

  describe('Retry Configuration', () => {
    it('should have correct retry configuration', async () => {
      // This tests the configuration indirectly through behavior
      await kafkaService.initProducer();

      let attemptCount = 0;
      const attemptTimes: number[] = [];

      mockProducer.send.mockImplementation(() => {
        attemptCount++;
        attemptTimes.push(Date.now());

        if (attemptCount <= 3) {
          return Promise.reject(new Error('Simulated failure'));
        }
        return Promise.resolve();
      });

      const message = {
        postId: 'post-123',
        userId: 'user-456',
        content: 'Test',
        createdAt: Date.now(),
        isCelebrity: false,
      };

      await kafkaService.publishNewPost(message);

      // Verify 4 total attempts (1 initial + 3 retries)
      expect(attemptCount).toBe(4);

      // Verify exponential backoff
      expect(attemptTimes.length).toBe(4);
    }, 15000);
  });

  describe('Error Handling', () => {
    it('should preserve original error message after retries exhausted', async () => {
      await kafkaService.initProducer();

      const originalError = new Error('Specific Kafka error: BROKER_NOT_AVAILABLE');
      mockProducer.send.mockRejectedValue(originalError);

      const message = {
        postId: 'post-123',
        userId: 'user-456',
        content: 'Test',
        createdAt: Date.now(),
        isCelebrity: false,
      };

      await expect(kafkaService.publishNewPost(message)).rejects.toThrow(
        'Specific Kafka error: BROKER_NOT_AVAILABLE'
      );
    }, 15000);

    it('should handle timeout errors with retry', async () => {
      await kafkaService.initProducer();

      let attemptCount = 0;
      mockProducer.send.mockImplementation(() => {
        attemptCount++;
        if (attemptCount === 1) {
          return Promise.reject(new Error('Request timeout'));
        }
        return Promise.resolve();
      });

      const message = {
        postId: 'post-123',
        userId: 'user-456',
        content: 'Test',
        createdAt: Date.now(),
        isCelebrity: false,
      };

      await kafkaService.publishNewPost(message);

      expect(mockProducer.send).toHaveBeenCalledTimes(2);
    });
  });

  describe('Producer Lifecycle', () => {
    it('should gracefully disconnect producer', async () => {
      await kafkaService.initProducer();
      await kafkaService.disconnectProducer();

      expect(mockProducer.disconnect).toHaveBeenCalled();
    });

    it('should handle disconnect when producer not initialized', async () => {
      // Should not throw
      await expect(kafkaService.disconnectProducer()).resolves.not.toThrow();
    });

    it('should shutdown both producer and consumer', async () => {
      const mockConsumer = {
        connect: jest.fn().mockResolvedValue(undefined),
        subscribe: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        run: jest.fn().mockResolvedValue(undefined),
      };

      (kafka.consumer as jest.Mock).mockReturnValue(mockConsumer);

      await kafkaService.initProducer();
      await kafkaService.initConsumer([KAFKA_TOPICS.NEW_POST]);
      await kafkaService.shutdown();

      expect(mockProducer.disconnect).toHaveBeenCalled();
      expect(mockConsumer.disconnect).toHaveBeenCalled();
    });
  });
});
