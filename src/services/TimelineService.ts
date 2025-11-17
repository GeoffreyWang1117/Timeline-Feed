import { cacheService } from './CacheService';
import { postService } from './PostService';
import { followService } from './FollowService';
import { User } from '../models/User';
import { logger } from '../utils/logger';
import { TimelinePost, PaginationInfo } from '../types';
import config from '../config';

export class TimelineService {
  /**
   * Get user's timeline (home feed)
   * Combines posts from followed users
   */
  async getTimeline(
    userId: string,
    limit: number,
    cursor?: string
  ): Promise<{ posts: TimelinePost[]; pagination: PaginationInfo }> {
    try {
      // Try to get timeline from cache first
      const { postIds, nextCursor } = await cacheService.getTimeline(userId, limit, cursor);

      let posts: TimelinePost[] = [];

      if (postIds.length > 0) {
        // Get post details
        posts = await postService.getPostsByIds(postIds, userId);
      }

      // If cache miss or insufficient posts, fall back to pull model
      if (posts.length < limit && !cursor) {
        const pulledPosts = await this.pullTimelineFromFollowing(userId, limit);
        posts = this.mergePosts(posts, pulledPosts, limit);

        // Pre-populate cache
        if (pulledPosts.length > 0) {
          await cacheService.addMultipleToTimeline(
            userId,
            pulledPosts.map((p) => ({
              postId: p.postId,
              timestamp: new Date(p.createdAt).getTime(),
            }))
          );
        }
      }

      return {
        posts,
        pagination: {
          nextCursor,
          hasMore: !!nextCursor || posts.length === limit,
          limit,
        },
      };
    } catch (error) {
      logger.error('Error getting timeline:', error);
      throw error;
    }
  }

  /**
   * Pull timeline from following users (for cache miss or celebrities)
   */
  private async pullTimelineFromFollowing(
    userId: string,
    limit: number
  ): Promise<TimelinePost[]> {
    try {
      // Get users that this user follows
      const followingIds = await followService.getFollowing(userId, 100);

      if (followingIds.length === 0) {
        return [];
      }

      // Check which are celebrities
      const users = await User.find({
        userId: { $in: followingIds },
        isCelebrity: true,
      }).lean();

      const celebrityIds = users.map((u) => u.userId);

      if (celebrityIds.length === 0) {
        return [];
      }

      // Get recent posts from celebrities
      const posts: TimelinePost[] = [];
      for (const celebrityId of celebrityIds) {
        const celebrityPosts = await postService.getUserPosts(celebrityId, 10);
        posts.push(...celebrityPosts);
      }

      // Sort by creation time and limit
      posts.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

      return posts.slice(0, limit);
    } catch (error) {
      logger.error('Error pulling timeline from following:', error);
      return [];
    }
  }

  /**
   * Merge and deduplicate posts
   */
  private mergePosts(
    cachedPosts: TimelinePost[],
    pulledPosts: TimelinePost[],
    limit: number
  ): TimelinePost[] {
    const postMap = new Map<string, TimelinePost>();

    // Add cached posts first (priority)
    cachedPosts.forEach((post) => postMap.set(post.postId, post));

    // Add pulled posts if not duplicate
    pulledPosts.forEach((post) => {
      if (!postMap.has(post.postId)) {
        postMap.set(post.postId, post);
      }
    });

    // Convert to array and sort by timestamp
    const merged = Array.from(postMap.values());
    merged.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

    return merged.slice(0, limit);
  }

  /**
   * Get trending/hot timeline
   */
  async getTrendingTimeline(
    limit: number,
    cursor?: string
  ): Promise<{ posts: TimelinePost[]; pagination: PaginationInfo }> {
    try {
      const postIds = await cacheService.getTrendingPosts(limit);
      const posts = await postService.getPostsByIds(postIds);

      return {
        posts,
        pagination: {
          hasMore: postIds.length === limit,
          limit,
        },
      };
    } catch (error) {
      logger.error('Error getting trending timeline:', error);
      throw error;
    }
  }

  /**
   * Refresh user's timeline cache (can be called periodically)
   */
  async refreshTimelineCache(userId: string): Promise<void> {
    try {
      logger.info(`Refreshing timeline cache for user ${userId}`);

      // Clear existing cache
      await cacheService.clearTimeline(userId);

      // Pull fresh posts
      const posts = await this.pullTimelineFromFollowing(userId, config.cache.timelineCacheSize);

      // Populate cache
      if (posts.length > 0) {
        await cacheService.addMultipleToTimeline(
          userId,
          posts.map((p) => ({
            postId: p.postId,
            timestamp: new Date(p.createdAt).getTime(),
          }))
        );
      }

      logger.info(`Timeline cache refreshed for user ${userId}: ${posts.length} posts`);
    } catch (error) {
      logger.error('Error refreshing timeline cache:', error);
      throw error;
    }
  }
}

export const timelineService = new TimelineService();
