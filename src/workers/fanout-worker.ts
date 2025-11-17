import { connectMongoDB, KAFKA_TOPICS } from '../config/database';
import { kafkaService } from '../services/KafkaService';
import { followService } from '../services/FollowService';
import { cacheService } from '../services/CacheService';
import { logger } from '../utils/logger';
import { FanoutMessage } from '../types';
import config from '../config';

/**
 * Fanout Worker
 * Consumes new post events and distributes them to followers' timelines
 */
class FanoutWorker {
  async start(): Promise<void> {
    try {
      // Connect to MongoDB
      await connectMongoDB();

      // Initialize Kafka consumer
      await kafkaService.initConsumer([KAFKA_TOPICS.NEW_POST]);

      // Start consuming messages
      await kafkaService.consumeMessages(async ({ topic, partition, message }) => {
        if (topic === KAFKA_TOPICS.NEW_POST) {
          await this.handleNewPost(message.value);
        }
      });

      logger.info('Fanout worker started successfully');

      // Handle shutdown
      process.on('SIGTERM', () => this.shutdown());
      process.on('SIGINT', () => this.shutdown());
    } catch (error) {
      logger.error('Failed to start fanout worker:', error);
      process.exit(1);
    }
  }

  /**
   * Handle new post event
   */
  private async handleNewPost(messageValue: Buffer | null): Promise<void> {
    if (!messageValue) {
      return;
    }

    try {
      const message: FanoutMessage = JSON.parse(messageValue.toString());
      logger.info(`Processing fanout for post ${message.postId} by user ${message.userId}`);

      // Determine fanout strategy
      if (message.isCelebrity) {
        // Pull model for celebrities - just log, followers will pull on demand
        logger.info(`Post ${message.postId} is from celebrity, using pull model`);
        // Optionally cache in a celebrity posts list
        await cacheService.addToTrending(message.postId, 10); // Higher score for celebrity
      } else {
        // Push model for normal users
        await this.pushToFollowers(message);
      }

      // Publish fanout complete event
      const followers = await followService.getAllFollowers(message.userId);
      await kafkaService.publishFanoutComplete(
        message.postId,
        message.userId,
        followers.length
      );

      logger.info(`Fanout complete for post ${message.postId}`);
    } catch (error) {
      logger.error('Error handling new post:', error);
      throw error;
    }
  }

  /**
   * Push post to all followers (batch processing)
   */
  private async pushToFollowers(message: FanoutMessage): Promise<void> {
    try {
      // Get all followers
      const followers = await followService.getAllFollowers(message.userId);

      if (followers.length === 0) {
        logger.debug(`No followers for user ${message.userId}`);
        return;
      }

      logger.info(`Pushing post ${message.postId} to ${followers.length} followers`);

      // Batch processing
      const batchSize = config.fanout.batchSize;
      for (let i = 0; i < followers.length; i += batchSize) {
        const batch = followers.slice(i, i + batchSize);

        // Push to each follower's timeline in parallel
        await Promise.all(
          batch.map(async (followerId) => {
            try {
              await cacheService.addToTimeline(
                followerId,
                message.postId,
                message.createdAt
              );
            } catch (error) {
              logger.error(`Error pushing to follower ${followerId}:`, error);
              // Continue with other followers
            }
          })
        );

        logger.debug(`Processed batch ${i / batchSize + 1}/${Math.ceil(followers.length / batchSize)}`);
      }

      logger.info(`Successfully pushed post ${message.postId} to ${followers.length} followers`);
    } catch (error) {
      logger.error('Error pushing to followers:', error);
      throw error;
    }
  }

  /**
   * Graceful shutdown
   */
  private async shutdown(): Promise<void> {
    logger.info('Shutting down fanout worker...');
    await kafkaService.shutdown();
    process.exit(0);
  }
}

// Start worker if this file is run directly
if (require.main === module) {
  const worker = new FanoutWorker();
  worker.start().catch((error) => {
    logger.error('Fatal error in fanout worker:', error);
    process.exit(1);
  });
}

export default FanoutWorker;
