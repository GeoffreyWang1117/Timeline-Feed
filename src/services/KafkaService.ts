import { kafka, KAFKA_TOPICS } from '../config/database';
import { Producer, Consumer, EachMessagePayload } from 'kafkajs';
import { logger } from '../utils/logger';
import { FanoutMessage } from '../types';
import config from '../config';

export class KafkaService {
  private producer: Producer | null = null;
  private consumer: Consumer | null = null;

  /**
   * Initialize producer
   */
  async initProducer(): Promise<void> {
    try {
      this.producer = kafka.producer();
      await this.producer.connect();
      logger.info('Kafka producer connected');
    } catch (error) {
      logger.error('Failed to connect Kafka producer:', error);
      throw error;
    }
  }

  /**
   * Initialize consumer
   */
  async initConsumer(topics: string[]): Promise<void> {
    try {
      this.consumer = kafka.consumer({ groupId: config.kafka.groupId });
      await this.consumer.connect();

      for (const topic of topics) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
      }

      logger.info(`Kafka consumer connected and subscribed to: ${topics.join(', ')}`);
    } catch (error) {
      logger.error('Failed to connect Kafka consumer:', error);
      throw error;
    }
  }

  /**
   * Publish new post event
   */
  async publishNewPost(message: FanoutMessage): Promise<void> {
    try {
      if (!this.producer) {
        await this.initProducer();
      }

      await this.producer!.send({
        topic: KAFKA_TOPICS.NEW_POST,
        messages: [
          {
            key: message.userId,
            value: JSON.stringify(message),
            timestamp: Date.now().toString(),
          },
        ],
      });

      logger.debug(`Published new post event: ${message.postId}`);
    } catch (error) {
      logger.error('Error publishing new post:', error);
      throw error;
    }
  }

  /**
   * Publish fanout complete event
   */
  async publishFanoutComplete(postId: string, userId: string, followerCount: number): Promise<void> {
    try {
      if (!this.producer) {
        await this.initProducer();
      }

      await this.producer!.send({
        topic: KAFKA_TOPICS.FANOUT_COMPLETE,
        messages: [
          {
            key: postId,
            value: JSON.stringify({
              postId,
              userId,
              followerCount,
              completedAt: Date.now(),
            }),
          },
        ],
      });

      logger.debug(`Published fanout complete event: ${postId}`);
    } catch (error) {
      logger.error('Error publishing fanout complete:', error);
      throw error;
    }
  }

  /**
   * Publish hot content event
   */
  async publishHotContent(postId: string, metrics: any): Promise<void> {
    try {
      if (!this.producer) {
        await this.initProducer();
      }

      await this.producer!.send({
        topic: KAFKA_TOPICS.HOT_CONTENT,
        messages: [
          {
            key: postId,
            value: JSON.stringify({
              postId,
              metrics,
              detectedAt: Date.now(),
            }),
          },
        ],
      });

      logger.debug(`Published hot content event: ${postId}`);
    } catch (error) {
      logger.error('Error publishing hot content:', error);
      throw error;
    }
  }

  /**
   * Consume messages
   */
  async consumeMessages(
    handler: (payload: EachMessagePayload) => Promise<void>
  ): Promise<void> {
    try {
      if (!this.consumer) {
        throw new Error('Consumer not initialized');
      }

      await this.consumer.run({
        eachMessage: async (payload) => {
          try {
            await handler(payload);
          } catch (error) {
            logger.error('Error handling message:', error);
            // Don't rethrow - continue processing other messages
          }
        },
      });

      logger.info('Kafka consumer started');
    } catch (error) {
      logger.error('Error consuming messages:', error);
      throw error;
    }
  }

  /**
   * Disconnect producer
   */
  async disconnectProducer(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      logger.info('Kafka producer disconnected');
    }
  }

  /**
   * Disconnect consumer
   */
  async disconnectConsumer(): Promise<void> {
    if (this.consumer) {
      await this.consumer.disconnect();
      logger.info('Kafka consumer disconnected');
    }
  }

  /**
   * Graceful shutdown
   */
  async shutdown(): Promise<void> {
    await Promise.all([this.disconnectProducer(), this.disconnectConsumer()]);
    logger.info('Kafka service shutdown complete');
  }
}

export const kafkaService = new KafkaService();
