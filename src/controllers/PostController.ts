import { Response } from 'express';
import { postService } from '../services/PostService';
import { AuthRequest } from '../middlewares/auth';
import { AppError, asyncHandler } from '../middlewares/errorHandler';
import Joi from 'joi';

const createPostSchema = Joi.object({
  content: Joi.string().required().min(1).max(280),
  mediaUrls: Joi.array().items(Joi.string().uri()).max(4),
});

export class PostController {
  /**
   * POST /api/v1/posts
   * Create a new post
   */
  createPost = asyncHandler(async (req: AuthRequest, res: Response) => {
    const userId = req.user?.userId;
    if (!userId) {
      throw new AppError('User not authenticated', 401, 'UNAUTHORIZED');
    }

    // Validate request body
    const { error, value } = createPostSchema.validate(req.body);
    if (error) {
      throw new AppError(error.details[0].message, 400, 'VALIDATION_ERROR');
    }

    const { content, mediaUrls } = value;

    const post = await postService.createPost(userId, content, mediaUrls);

    res.status(201).json({
      success: true,
      data: { post },
    });
  });

  /**
   * GET /api/v1/posts/:postId
   * Get a single post
   */
  getPost = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { postId } = req.params;
    const viewerId = req.user?.userId;

    const post = await postService.getPost(postId, viewerId);

    if (!post) {
      throw new AppError('Post not found', 404, 'POST_NOT_FOUND');
    }

    res.json({
      success: true,
      data: { post },
    });
  });

  /**
   * POST /api/v1/posts/:postId/like
   * Like a post
   */
  likePost = asyncHandler(async (req: AuthRequest, res: Response) => {
    const userId = req.user?.userId;
    if (!userId) {
      throw new AppError('User not authenticated', 401, 'UNAUTHORIZED');
    }

    const { postId } = req.params;

    await postService.likePost(postId, userId);

    res.json({
      success: true,
      message: 'Post liked',
    });
  });

  /**
   * DELETE /api/v1/posts/:postId
   * Delete a post
   */
  deletePost = asyncHandler(async (req: AuthRequest, res: Response) => {
    const userId = req.user?.userId;
    if (!userId) {
      throw new AppError('User not authenticated', 401, 'UNAUTHORIZED');
    }

    const { postId } = req.params;

    await postService.deletePost(postId, userId);

    res.json({
      success: true,
      message: 'Post deleted',
    });
  });

  /**
   * GET /api/v1/users/:userId/posts
   * Get user's posts
   */
  getUserPosts = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { userId } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
    const offset = parseInt(req.query.offset as string) || 0;

    const posts = await postService.getUserPosts(userId, limit, offset);

    res.json({
      success: true,
      data: {
        posts,
        pagination: {
          limit,
          offset,
          hasMore: posts.length === limit,
        },
      },
    });
  });
}

export const postController = new PostController();
