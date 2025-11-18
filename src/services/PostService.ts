import { Post, IPost } from '../models/Post';
import { User } from '../models/User';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger';
import { cacheService } from './CacheService';
import { kafkaService } from './KafkaService';
import { TimelinePost } from '../types';

export class PostService {
  /**
   * Create a new post
   */
  async createPost(
    userId: string,
    content: string,
    mediaUrls?: string[]
  ): Promise<IPost> {
    try {
      // Get user info
      const user = await User.findOne({ userId });
      if (!user) {
        throw new Error('User not found');
      }

      // Extract hashtags and mentions
      const hashtags = this.extractHashtags(content);
      const mentions = this.extractMentions(content);

      // Create post
      const post = await Post.create({
        postId: uuidv4(),
        userId,
        content,
        mediaUrls: mediaUrls || [],
        hashtags,
        mentions,
        likeCount: 0,
        retweetCount: 0,
        replyCount: 0,
        viewCount: 0,
        isHot: false,
      });

      // Update user post count
      await User.updateOne({ userId }, { $inc: { postCount: 1 } });

      // Cache the post
      await cacheService.cachePost(post.postId, this.toTimelinePost(post, user));

      // Publish to Kafka for fanout
      await kafkaService.publishNewPost({
        postId: post.postId,
        userId: post.userId,
        content: post.content,
        createdAt: post.createdAt.getTime(),
        isCelebrity: user.isCelebrity,
      });

      logger.info(`Post created: ${post.postId} by user ${userId}`);

      return post;
    } catch (error) {
      logger.error('Error creating post:', error);
      throw error;
    }
  }

  /**
   * Get post by ID
   */
  async getPost(postId: string, viewerId?: string): Promise<TimelinePost | null> {
    try {
      // Try cache first
      const cached = await cacheService.getCachedPost(postId);
      if (cached) {
        // Increment views
        if (viewerId) {
          await Promise.all([
            cacheService.incrementPostViews(postId),
            cacheService.trackVisitor(postId, viewerId),
          ]);

          // Check if it's hot content
          const isHot = await cacheService.isHotContent(postId);
          if (isHot && !cached.isHot) {
            await Post.updateOne({ postId }, { $set: { isHot: true } });
            cached.isHot = true;
          }
        }

        return cached;
      }

      // Fallback to MongoDB
      const post = await Post.findOne({ postId }).lean();
      if (!post) {
        return null;
      }

      const user = await User.findOne({ userId: post.userId }).lean();
      if (!user) {
        return null;
      }

      const timelinePost = this.toTimelinePost(post, user);

      // Cache it
      await cacheService.cachePost(postId, timelinePost);

      // Track view
      if (viewerId) {
        await Promise.all([
          cacheService.incrementPostViews(postId),
          cacheService.trackVisitor(postId, viewerId),
        ]);
      }

      return timelinePost;
    } catch (error) {
      logger.error('Error getting post:', error);
      throw error;
    }
  }

  /**
   * Get multiple posts by IDs (optimized - fixed N+1 query)
   */
  async getPostsByIds(postIds: string[], viewerId?: string): Promise<TimelinePost[]> {
    if (postIds.length === 0) {
      return [];
    }

    try {
      const posts: TimelinePost[] = [];

      // Batch get from cache (fixes N+1 query - single MGET instead of N GETs)
      const cachedPostsMap = await cacheService.getCachedPosts(postIds);

      // Identify uncached posts
      const uncachedPostIds = postIds.filter((id) => !cachedPostsMap.has(id));

      // Add cached posts to result
      cachedPostsMap.forEach((post) => {
        posts.push(post);
      });

      // Fetch uncached posts from MongoDB in batch (already optimized)
      if (uncachedPostIds.length > 0) {
        const dbPosts = await Post.find({ postId: { $in: uncachedPostIds } }).lean();
        const userIds = [...new Set(dbPosts.map((p) => p.userId))];
        const users = await User.find({ userId: { $in: userIds } }).lean();

        const userMap = new Map(users.map((u) => [u.userId, u]));

        const timelinePosts = dbPosts.map((post) => {
          const user = userMap.get(post.userId);
          return this.toTimelinePost(post, user!);
        });

        posts.push(...timelinePosts);

        // Cache them in batch
        await cacheService.cacheMultiplePosts(
          timelinePosts.map((p) => ({ postId: p.postId, data: p }))
        );
      }

      // Track views in parallel (already optimized)
      if (viewerId) {
        const trackingPromises = postIds.map((postId) =>
          Promise.all([
            cacheService.incrementPostViews(postId),
            cacheService.trackVisitor(postId, viewerId),
          ])
        );
        await Promise.all(trackingPromises);
      }

      // Sort by creation time (newest first)
      posts.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

      return posts;
    } catch (error) {
      logger.error('Error getting posts by IDs:', error);
      throw error;
    }
  }

  /**
   * Get user's posts
   */
  async getUserPosts(userId: string, limit = 20, offset = 0): Promise<TimelinePost[]> {
    try {
      const posts = await Post.find({ userId })
        .sort({ createdAt: -1 })
        .skip(offset)
        .limit(limit)
        .lean();

      const user = await User.findOne({ userId }).lean();
      if (!user) {
        return [];
      }

      return posts.map((post) => this.toTimelinePost(post, user));
    } catch (error) {
      logger.error('Error getting user posts:', error);
      throw error;
    }
  }

  /**
   * Like a post
   */
  async likePost(postId: string, userId: string): Promise<void> {
    try {
      const result = await Post.updateOne(
        { postId },
        { $inc: { likeCount: 1 } }
      );

      if (result.modifiedCount === 0) {
        throw new Error('Post not found');
      }

      // Invalidate cache
      await cacheService.cachePost(postId, await this.getPost(postId));

      // Update trending score
      await cacheService.addToTrending(postId, 1);

      logger.info(`Post ${postId} liked by user ${userId}`);
    } catch (error) {
      logger.error('Error liking post:', error);
      throw error;
    }
  }

  /**
   * Delete a post
   */
  async deletePost(postId: string, userId: string): Promise<void> {
    try {
      const post = await Post.findOne({ postId });
      if (!post) {
        throw new Error('Post not found');
      }

      if (post.userId !== userId) {
        throw new Error('Unauthorized');
      }

      await Post.deleteOne({ postId });

      // Update user post count
      await User.updateOne({ userId }, { $inc: { postCount: -1 } });

      logger.info(`Post ${postId} deleted by user ${userId}`);
    } catch (error) {
      logger.error('Error deleting post:', error);
      throw error;
    }
  }

  /**
   * Convert post to TimelinePost format
   */
  private toTimelinePost(post: any, user: any): TimelinePost {
    return {
      postId: post.postId,
      userId: post.userId,
      username: user.username,
      displayName: user.displayName,
      avatarUrl: user.avatarUrl,
      isVerified: user.isVerified,
      content: post.content,
      mediaUrls: post.mediaUrls,
      hashtags: post.hashtags,
      mentions: post.mentions,
      likeCount: post.likeCount,
      retweetCount: post.retweetCount,
      replyCount: post.replyCount,
      viewCount: post.viewCount,
      isHot: post.isHot,
      createdAt: post.createdAt,
    };
  }

  /**
   * Extract hashtags from content
   */
  private extractHashtags(content: string): string[] {
    const regex = /#(\w+)/g;
    const matches = content.match(regex);
    return matches ? matches.map((tag) => tag.slice(1).toLowerCase()) : [];
  }

  /**
   * Extract mentions from content
   */
  private extractMentions(content: string): string[] {
    const regex = /@(\w+)/g;
    const matches = content.match(regex);
    return matches ? matches.map((mention) => mention.slice(1).toLowerCase()) : [];
  }
}

export const postService = new PostService();
