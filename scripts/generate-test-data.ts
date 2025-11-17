import { connectMongoDB } from '../src/config/database';
import { User } from '../src/models/User';
import { Post } from '../src/models/Post';
import { Follow } from '../src/models/Follow';
import { logger } from '../src/utils/logger';
import { v4 as uuidv4 } from 'uuid';

/**
 * Generate test data for development
 */
class TestDataGenerator {
  async generate(
    userCount: number = 100,
    postsPerUser: number = 10,
    followsPerUser: number = 20
  ): Promise<void> {
    logger.info('Generating test data...');
    logger.info(`Users: ${userCount}, Posts per user: ${postsPerUser}, Follows per user: ${followsPerUser}`);

    // Clear existing data
    await this.clearData();

    // Generate users
    const users = await this.generateUsers(userCount);
    logger.info(`Generated ${users.length} users`);

    // Generate follows
    await this.generateFollows(users, followsPerUser);
    logger.info('Generated follow relationships');

    // Generate posts
    await this.generatePosts(users, postsPerUser);
    logger.info('Generated posts');

    // Mark celebrities
    await this.markCelebrities();

    logger.info('Test data generation completed');
  }

  private async clearData(): Promise<void> {
    await Promise.all([
      User.deleteMany({}),
      Post.deleteMany({}),
      Follow.deleteMany({}),
    ]);
    logger.info('Cleared existing data');
  }

  private async generateUsers(count: number): Promise<any[]> {
    const users = [];

    for (let i = 0; i < count; i++) {
      users.push({
        userId: uuidv4(),
        username: `testuser${i}`,
        email: `testuser${i}@example.com`,
        displayName: `Test User ${i}`,
        bio: `This is test user ${i}`,
        isVerified: i % 10 === 0, // 10% verified
        isCelebrity: false,
        followerCount: 0,
        followingCount: 0,
        postCount: 0,
      });
    }

    await User.insertMany(users);
    return users;
  }

  private async generateFollows(users: any[], followsPerUser: number): Promise<void> {
    const follows = [];

    for (const user of users) {
      // Select random users to follow
      const toFollow = this.selectRandomUsers(users, user.userId, followsPerUser);

      for (const following of toFollow) {
        follows.push({
          followerId: user.userId,
          followingId: following.userId,
        });
      }
    }

    // Batch insert
    const batchSize = 1000;
    for (let i = 0; i < follows.length; i += batchSize) {
      const batch = follows.slice(i, i + batchSize);
      await Follow.insertMany(batch, { ordered: false }).catch(() => {
        // Ignore duplicates
      });
    }

    // Update counts
    for (const user of users) {
      const followerCount = await Follow.countDocuments({ followingId: user.userId });
      const followingCount = await Follow.countDocuments({ followerId: user.userId });

      await User.updateOne(
        { userId: user.userId },
        { $set: { followerCount, followingCount } }
      );
    }
  }

  private async generatePosts(users: any[], postsPerUser: number): Promise<void> {
    const posts = [];

    const sampleContents = [
      'Just had the best coffee!',
      'Working on an exciting new project',
      'Beautiful day outside',
      'Can\'t wait for the weekend!',
      'Learning something new every day',
      'Grateful for all the support',
      'This is amazing! #blessed',
      'New blog post coming soon',
      'Thoughts on the latest tech trends',
      'Coffee, code, repeat',
    ];

    for (const user of users) {
      for (let i = 0; i < postsPerUser; i++) {
        const content = sampleContents[Math.floor(Math.random() * sampleContents.length)];
        const randomDaysAgo = Math.floor(Math.random() * 30);
        const createdAt = new Date();
        createdAt.setDate(createdAt.getDate() - randomDaysAgo);

        posts.push({
          postId: uuidv4(),
          userId: user.userId,
          content,
          hashtags: content.includes('#') ? ['blessed'] : [],
          likeCount: Math.floor(Math.random() * 100),
          retweetCount: Math.floor(Math.random() * 50),
          replyCount: Math.floor(Math.random() * 20),
          viewCount: Math.floor(Math.random() * 500),
          isHot: false,
          createdAt,
        });
      }
    }

    // Batch insert
    const batchSize = 1000;
    for (let i = 0; i < posts.length; i += batchSize) {
      const batch = posts.slice(i, i + batchSize);
      await Post.insertMany(batch);
    }

    // Update post counts
    for (const user of users) {
      const postCount = await Post.countDocuments({ userId: user.userId });
      await User.updateOne({ userId: user.userId }, { $set: { postCount } });
    }
  }

  private async markCelebrities(): Promise<void> {
    const result = await User.updateMany(
      { followerCount: { $gte: 10000 } },
      { $set: { isCelebrity: true } }
    );

    logger.info(`Marked ${result.modifiedCount} users as celebrities`);
  }

  private selectRandomUsers(users: any[], excludeId: string, count: number): any[] {
    const available = users.filter((u) => u.userId !== excludeId);
    const shuffled = available.sort(() => Math.random() - 0.5);
    return shuffled.slice(0, Math.min(count, available.length));
  }
}

// CLI
async function main() {
  try {
    await connectMongoDB();

    const userCount = parseInt(process.argv[2]) || 100;
    const postsPerUser = parseInt(process.argv[3]) || 10;
    const followsPerUser = parseInt(process.argv[4]) || 20;

    const generator = new TestDataGenerator();
    await generator.generate(userCount, postsPerUser, followsPerUser);

    process.exit(0);
  } catch (error) {
    logger.error('Error generating test data:', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

export default TestDataGenerator;
