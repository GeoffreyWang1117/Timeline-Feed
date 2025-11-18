import { Follow, IFollow } from '../models/Follow';
import { User } from '../models/User';
import { redis, REDIS_KEYS, CACHE_TTL } from '../config/database';
import { logger } from '../utils/logger';
import config from '../config';

export class FollowService {
  /**
   * Follow a user
   */
  async followUser(followerId: string, followingId: string): Promise<void> {
    if (followerId === followingId) {
      throw new Error('Users cannot follow themselves');
    }

    try {
      // Create follow relationship in MongoDB
      await Follow.create({
        followerId,
        followingId,
      });

      // Update follower/following counts
      await Promise.all([
        User.findOneAndUpdate(
          { userId: followerId },
          { $inc: { followingCount: 1 } }
        ),
        User.findOneAndUpdate(
          { userId: followingId },
          { $inc: { followerCount: 1 } }
        ),
      ]);

      // Update celebrity status if threshold crossed
      const followingUser = await User.findOne({ userId: followingId });
      if (
        followingUser &&
        followingUser.followerCount >= config.fanout.celebrityThreshold &&
        !followingUser.isCelebrity
      ) {
        await User.updateOne(
          { userId: followingId },
          { $set: { isCelebrity: true } }
        );
        logger.info(`User ${followingId} marked as celebrity`);
      }

      // Update Redis cache
      await Promise.all([
        redis.sadd(REDIS_KEYS.FOLLOWERS(followingId), followerId),
        redis.sadd(REDIS_KEYS.FOLLOWING(followerId), followingId),
        redis.expire(REDIS_KEYS.FOLLOWERS(followingId), CACHE_TTL.FOLLOWERS),
        redis.expire(REDIS_KEYS.FOLLOWING(followerId), CACHE_TTL.FOLLOWERS),
      ]);

      logger.info(`User ${followerId} followed ${followingId}`);
    } catch (error: any) {
      if (error.code === 11000) {
        throw new Error('Already following this user');
      }
      throw error;
    }
  }

  /**
   * Unfollow a user
   */
  async unfollowUser(followerId: string, followingId: string): Promise<void> {
    try {
      // Delete follow relationship
      const result = await Follow.deleteOne({
        followerId,
        followingId,
      });

      if (result.deletedCount === 0) {
        throw new Error('Not following this user');
      }

      // Update follower/following counts
      await Promise.all([
        User.findOneAndUpdate(
          { userId: followerId },
          { $inc: { followingCount: -1 } }
        ),
        User.findOneAndUpdate(
          { userId: followingId },
          { $inc: { followerCount: -1 } }
        ),
      ]);

      // Update Redis cache
      await Promise.all([
        redis.srem(REDIS_KEYS.FOLLOWERS(followingId), followerId),
        redis.srem(REDIS_KEYS.FOLLOWING(followerId), followingId),
      ]);

      logger.info(`User ${followerId} unfollowed ${followingId}`);
    } catch (error) {
      throw error;
    }
  }

  /**
   * Get followers of a user (with cache)
   */
  async getFollowers(userId: string, limit = 100, offset = 0): Promise<string[]> {
    try {
      // Try Redis cache first
      const cacheKey = REDIS_KEYS.FOLLOWERS(userId);
      const cachedFollowers = await redis.smembers(cacheKey);

      if (cachedFollowers.length > 0) {
        return cachedFollowers.slice(offset, offset + limit);
      }

      // Fallback to MongoDB
      const followers = await Follow.find({ followingId: userId })
        .select('followerId')
        .skip(offset)
        .limit(limit)
        .lean();

      const followerIds = followers.map((f) => f.followerId);

      // Update cache
      if (followerIds.length > 0) {
        await redis.sadd(cacheKey, ...followerIds);
        await redis.expire(cacheKey, CACHE_TTL.FOLLOWERS);
      }

      return followerIds;
    } catch (error) {
      logger.error('Error getting followers:', error);
      throw error;
    }
  }

  /**
   * Get all followers (for fanout)
   */
  async getAllFollowers(userId: string): Promise<string[]> {
    try {
      // Try Redis cache first
      const cacheKey = REDIS_KEYS.FOLLOWERS(userId);
      const cachedFollowers = await redis.smembers(cacheKey);

      if (cachedFollowers.length > 0) {
        return cachedFollowers;
      }

      // Fallback to MongoDB
      const followers = await Follow.find({ followingId: userId })
        .select('followerId')
        .lean();

      const followerIds = followers.map((f) => f.followerId);

      // Update cache
      if (followerIds.length > 0) {
        await redis.sadd(cacheKey, ...followerIds);
        await redis.expire(cacheKey, CACHE_TTL.FOLLOWERS);
      }

      return followerIds;
    } catch (error) {
      logger.error('Error getting all followers:', error);
      throw error;
    }
  }

  /**
   * Get users that a user is following
   */
  async getFollowing(userId: string, limit = 100, offset = 0): Promise<string[]> {
    try {
      // Try Redis cache first
      const cacheKey = REDIS_KEYS.FOLLOWING(userId);
      const cachedFollowing = await redis.smembers(cacheKey);

      if (cachedFollowing.length > 0) {
        return cachedFollowing.slice(offset, offset + limit);
      }

      // Fallback to MongoDB
      const following = await Follow.find({ followerId: userId })
        .select('followingId')
        .skip(offset)
        .limit(limit)
        .lean();

      const followingIds = following.map((f) => f.followingId);

      // Update cache
      if (followingIds.length > 0) {
        await redis.sadd(cacheKey, ...followingIds);
        await redis.expire(cacheKey, CACHE_TTL.FOLLOWERS);
      }

      return followingIds;
    } catch (error) {
      logger.error('Error getting following:', error);
      throw error;
    }
  }

  /**
   * Check if a user is following another user
   */
  async isFollowing(followerId: string, followingId: string): Promise<boolean> {
    try {
      // Try Redis cache first
      const cacheKey = REDIS_KEYS.FOLLOWING(followerId);
      const exists = await redis.sismember(cacheKey, followingId);

      if (exists === 1) {
        return true;
      }

      // Fallback to MongoDB
      const follow = await Follow.findOne({
        followerId,
        followingId,
      }).lean();

      return !!follow;
    } catch (error) {
      logger.error('Error checking follow status:', error);
      throw error;
    }
  }

  /**
   * Get follower count
   */
  async getFollowerCount(userId: string): Promise<number> {
    const user = await User.findOne({ userId }).select('followerCount').lean();
    return user?.followerCount || 0;
  }

  /**
   * Get following count
   */
  async getFollowingCount(userId: string): Promise<number> {
    const user = await User.findOne({ userId }).select('followingCount').lean();
    return user?.followingCount || 0;
  }

  /**
   * Get followers with user details (optimized with aggregation - fixes N+1)
   * Single query instead of 2 separate queries
   */
  async getFollowersWithDetails(
    userId: string,
    limit = 100,
    offset = 0
  ): Promise<Array<{
    userId: string;
    username: string;
    displayName: string;
    avatarUrl?: string;
    isVerified: boolean;
  }>> {
    try {
      const followers = await Follow.aggregate([
        // Match followers of the target user
        { $match: { followingId: userId } },

        // Pagination
        { $skip: offset },
        { $limit: limit },

        // Join with User collection to get follower details
        {
          $lookup: {
            from: 'users',
            localField: 'followerId',
            foreignField: 'userId',
            as: 'followerUser',
          },
        },

        // Unwind the array
        { $unwind: '$followerUser' },

        // Project only needed fields
        {
          $project: {
            _id: 0,
            userId: '$followerUser.userId',
            username: '$followerUser.username',
            displayName: '$followerUser.displayName',
            avatarUrl: '$followerUser.avatarUrl',
            isVerified: '$followerUser.isVerified',
          },
        },
      ]);

      return followers;
    } catch (error) {
      logger.error('Error getting followers with details:', error);
      throw error;
    }
  }

  /**
   * Get following with user details (optimized with aggregation - fixes N+1)
   * Single query instead of 2 separate queries
   */
  async getFollowingWithDetails(
    userId: string,
    limit = 100,
    offset = 0
  ): Promise<Array<{
    userId: string;
    username: string;
    displayName: string;
    avatarUrl?: string;
    isVerified: boolean;
  }>> {
    try {
      const following = await Follow.aggregate([
        // Match users that the target user is following
        { $match: { followerId: userId } },

        // Pagination
        { $skip: offset },
        { $limit: limit },

        // Join with User collection to get following user details
        {
          $lookup: {
            from: 'users',
            localField: 'followingId',
            foreignField: 'userId',
            as: 'followingUser',
          },
        },

        // Unwind the array
        { $unwind: '$followingUser' },

        // Project only needed fields
        {
          $project: {
            _id: 0,
            userId: '$followingUser.userId',
            username: '$followingUser.username',
            displayName: '$followingUser.displayName',
            avatarUrl: '$followingUser.avatarUrl',
            isVerified: '$followingUser.isVerified',
          },
        },
      ]);

      return following;
    } catch (error) {
      logger.error('Error getting following with details:', error);
      throw error;
    }
  }
}

export const followService = new FollowService();
