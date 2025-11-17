import fs from 'fs';
import path from 'path';
import csvParser from 'csv-parser';
import { connectMongoDB } from '../config/database';
import { User } from '../models/User';
import { Post } from '../models/Post';
import { Follow } from '../models/Follow';
import { logger } from '../utils/logger';
import { v4 as uuidv4 } from 'uuid';

interface TwitterRecord {
  tweet_id: string;
  user_id: string;
  user_name: string;
  text: string;
  created_at: string;
  retweet_count?: number;
  favorite_count?: number;
}

class DataSeeder {
  /**
   * Seed database with Twitter dataset
   */
  async seedTwitterData(filePath: string): Promise<void> {
    logger.info('Starting Twitter data import...');

    const users = new Map<string, any>();
    const posts: any[] = [];

    return new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(csvParser())
        .on('data', (row: TwitterRecord) => {
          try {
            // Extract user
            if (!users.has(row.user_id)) {
              users.set(row.user_id, {
                userId: row.user_id,
                username: row.user_name || `user_${row.user_id}`,
                email: `${row.user_id}@example.com`,
                displayName: row.user_name || `User ${row.user_id}`,
                followerCount: 0,
                followingCount: 0,
                postCount: 0,
                isVerified: false,
                isCelebrity: false,
              });
            }

            // Extract post
            posts.push({
              postId: row.tweet_id || uuidv4(),
              userId: row.user_id,
              content: row.text?.substring(0, 280) || '',
              likeCount: parseInt(row.favorite_count as any) || 0,
              retweetCount: parseInt(row.retweet_count as any) || 0,
              replyCount: 0,
              viewCount: 0,
              isHot: false,
              createdAt: row.created_at ? new Date(row.created_at) : new Date(),
              hashtags: this.extractHashtags(row.text),
              mentions: this.extractMentions(row.text),
            });
          } catch (error) {
            logger.error('Error parsing row:', error);
          }
        })
        .on('end', async () => {
          try {
            await this.importData(Array.from(users.values()), posts);
            logger.info('Twitter data import completed');
            resolve();
          } catch (error) {
            reject(error);
          }
        })
        .on('error', reject);
    });
  }

  /**
   * Import users and posts to database
   */
  private async importData(users: any[], posts: any[]): Promise<void> {
    logger.info(`Importing ${users.length} users and ${posts.length} posts...`);

    // Batch insert users
    const userBatchSize = 1000;
    for (let i = 0; i < users.length; i += userBatchSize) {
      const batch = users.slice(i, i + userBatchSize);
      await User.insertMany(batch, { ordered: false }).catch((error) => {
        if (error.code !== 11000) {
          throw error;
        }
        // Ignore duplicate key errors
      });
      logger.info(`Imported users: ${Math.min(i + userBatchSize, users.length)}/${users.length}`);
    }

    // Update post counts
    const userPostCounts = new Map<string, number>();
    posts.forEach((post) => {
      userPostCounts.set(post.userId, (userPostCounts.get(post.userId) || 0) + 1);
    });

    for (const [userId, count] of userPostCounts) {
      await User.updateOne({ userId }, { $set: { postCount: count } });
    }

    // Batch insert posts
    const postBatchSize = 1000;
    for (let i = 0; i < posts.length; i += postBatchSize) {
      const batch = posts.slice(i, i + postBatchSize);
      await Post.insertMany(batch, { ordered: false }).catch((error) => {
        if (error.code !== 11000) {
          throw error;
        }
      });
      logger.info(`Imported posts: ${Math.min(i + postBatchSize, posts.length)}/${posts.length}`);
    }

    logger.info('Data import completed successfully');
  }

  /**
   * Generate random follow relationships
   */
  async generateFollowGraph(userCount: number, followsPerUser: number): Promise<void> {
    logger.info(`Generating follow graph for ${userCount} users...`);

    const users = await User.find().limit(userCount).select('userId').lean();
    const userIds = users.map((u) => u.userId);

    if (userIds.length === 0) {
      logger.warn('No users found in database');
      return;
    }

    const follows: any[] = [];

    for (const userId of userIds) {
      // Randomly select users to follow
      const toFollow = this.selectRandomUsers(userIds, userId, followsPerUser);

      toFollow.forEach((followingId) => {
        follows.push({
          followerId: userId,
          followingId,
        });
      });
    }

    // Batch insert follows
    const batchSize = 1000;
    for (let i = 0; i < follows.length; i += batchSize) {
      const batch = follows.slice(i, i + batchSize);
      await Follow.insertMany(batch, { ordered: false }).catch((error) => {
        if (error.code !== 11000) {
          throw error;
        }
      });
      logger.info(`Created follows: ${Math.min(i + batchSize, follows.length)}/${follows.length}`);
    }

    // Update follower/following counts
    await this.updateFollowCounts();

    // Mark celebrities (users with > 10k followers)
    await this.markCelebrities();

    logger.info('Follow graph generation completed');
  }

  /**
   * Update follower/following counts
   */
  private async updateFollowCounts(): Promise<void> {
    logger.info('Updating follower/following counts...');

    const pipeline = [
      {
        $group: {
          _id: '$followingId',
          count: { $sum: 1 },
        },
      },
    ];

    const followerCounts = await Follow.aggregate(pipeline);

    for (const { _id: userId, count } of followerCounts) {
      await User.updateOne({ userId }, { $set: { followerCount: count } });
    }

    const followingPipeline = [
      {
        $group: {
          _id: '$followerId',
          count: { $sum: 1 },
        },
      },
    ];

    const followingCounts = await Follow.aggregate(followingPipeline);

    for (const { _id: userId, count } of followingCounts) {
      await User.updateOne({ userId }, { $set: { followingCount: count } });
    }

    logger.info('Counts updated');
  }

  /**
   * Mark users with > 10k followers as celebrities
   */
  private async markCelebrities(): Promise<void> {
    const result = await User.updateMany(
      { followerCount: { $gte: 10000 } },
      { $set: { isCelebrity: true } }
    );

    logger.info(`Marked ${result.modifiedCount} users as celebrities`);
  }

  /**
   * Select random users (excluding self)
   */
  private selectRandomUsers(userIds: string[], excludeId: string, count: number): string[] {
    const available = userIds.filter((id) => id !== excludeId);
    const shuffled = available.sort(() => Math.random() - 0.5);
    return shuffled.slice(0, Math.min(count, available.length));
  }

  /**
   * Extract hashtags
   */
  private extractHashtags(text: string): string[] {
    if (!text) return [];
    const regex = /#(\w+)/g;
    const matches = text.match(regex);
    return matches ? matches.map((tag) => tag.slice(1).toLowerCase()) : [];
  }

  /**
   * Extract mentions
   */
  private extractMentions(text: string): string[] {
    if (!text) return [];
    const regex = /@(\w+)/g;
    const matches = text.match(regex);
    return matches ? matches.map((mention) => mention.slice(1).toLowerCase()) : [];
  }

  /**
   * Clear all data
   */
  async clearData(): Promise<void> {
    logger.warn('Clearing all data...');
    await Promise.all([
      User.deleteMany({}),
      Post.deleteMany({}),
      Follow.deleteMany({}),
    ]);
    logger.info('All data cleared');
  }
}

// CLI interface
async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  try {
    await connectMongoDB();
    const seeder = new DataSeeder();

    switch (command) {
      case 'twitter':
        const filePath = args[1] || './data/tweets.csv';
        if (!fs.existsSync(filePath)) {
          logger.error(`File not found: ${filePath}`);
          process.exit(1);
        }
        await seeder.seedTwitterData(filePath);
        break;

      case 'follow-graph':
        const userCount = parseInt(args[1]) || 1000;
        const followsPerUser = parseInt(args[2]) || 50;
        await seeder.generateFollowGraph(userCount, followsPerUser);
        break;

      case 'clear':
        await seeder.clearData();
        break;

      default:
        logger.info('Usage:');
        logger.info('  npm run seed twitter [file_path]       - Import Twitter dataset');
        logger.info('  npm run seed follow-graph [users] [follows] - Generate follow graph');
        logger.info('  npm run seed clear                     - Clear all data');
    }

    process.exit(0);
  } catch (error) {
    logger.error('Seed error:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

export default DataSeeder;
