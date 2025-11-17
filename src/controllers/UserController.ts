import { Response } from 'express';
import { followService } from '../services/FollowService';
import { User } from '../models/User';
import { AuthRequest, generateToken } from '../middlewares/auth';
import { AppError, asyncHandler } from '../middlewares/errorHandler';
import { v4 as uuidv4 } from 'uuid';
import bcrypt from 'bcryptjs';
import Joi from 'joi';

const registerSchema = Joi.object({
  username: Joi.string().required().min(3).max(30).lowercase(),
  email: Joi.string().required().email(),
  password: Joi.string().required().min(6),
  displayName: Joi.string().required().max(50),
});

export class UserController {
  /**
   * POST /api/v1/users/register
   * Register a new user
   */
  register = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { error, value } = registerSchema.validate(req.body);
    if (error) {
      throw new AppError(error.details[0].message, 400, 'VALIDATION_ERROR');
    }

    const { username, email, password, displayName } = value;

    // Check if user exists
    const existingUser = await User.findOne({
      $or: [{ username }, { email }],
    });

    if (existingUser) {
      throw new AppError('Username or email already exists', 409, 'USER_EXISTS');
    }

    // Hash password (in production, use proper password hashing)
    const hashedPassword = await bcrypt.hash(password, 10);

    // Create user
    const user = await User.create({
      userId: uuidv4(),
      username,
      email,
      displayName,
      // Note: Password should be stored in a separate auth table
      // This is simplified for demo purposes
    });

    // Generate token
    const token = generateToken(user.userId, user.username);

    res.status(201).json({
      success: true,
      data: {
        user: {
          userId: user.userId,
          username: user.username,
          displayName: user.displayName,
          email: user.email,
        },
        token,
      },
    });
  });

  /**
   * POST /api/v1/users/:userId/follow
   * Follow a user
   */
  followUser = asyncHandler(async (req: AuthRequest, res: Response) => {
    const followerId = req.user?.userId;
    if (!followerId) {
      throw new AppError('User not authenticated', 401, 'UNAUTHORIZED');
    }

    const { userId: followingId } = req.params;

    await followService.followUser(followerId, followingId);

    res.json({
      success: true,
      message: 'User followed',
    });
  });

  /**
   * DELETE /api/v1/users/:userId/follow
   * Unfollow a user
   */
  unfollowUser = asyncHandler(async (req: AuthRequest, res: Response) => {
    const followerId = req.user?.userId;
    if (!followerId) {
      throw new AppError('User not authenticated', 401, 'UNAUTHORIZED');
    }

    const { userId: followingId } = req.params;

    await followService.unfollowUser(followerId, followingId);

    res.json({
      success: true,
      message: 'User unfollowed',
    });
  });

  /**
   * GET /api/v1/users/:userId/followers
   * Get user's followers
   */
  getFollowers = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { userId } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);
    const offset = parseInt(req.query.offset as string) || 0;

    const followerIds = await followService.getFollowers(userId, limit, offset);
    const followers = await User.find({ userId: { $in: followerIds } })
      .select('userId username displayName avatarUrl isVerified')
      .lean();

    res.json({
      success: true,
      data: {
        followers,
        pagination: {
          limit,
          offset,
          hasMore: followerIds.length === limit,
        },
      },
    });
  });

  /**
   * GET /api/v1/users/:userId/following
   * Get users that a user is following
   */
  getFollowing = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { userId } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);
    const offset = parseInt(req.query.offset as string) || 0;

    const followingIds = await followService.getFollowing(userId, limit, offset);
    const following = await User.find({ userId: { $in: followingIds } })
      .select('userId username displayName avatarUrl isVerified')
      .lean();

    res.json({
      success: true,
      data: {
        following,
        pagination: {
          limit,
          offset,
          hasMore: followingIds.length === limit,
        },
      },
    });
  });

  /**
   * GET /api/v1/users/:userId
   * Get user profile
   */
  getProfile = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { userId } = req.params;

    const user = await User.findOne({ userId })
      .select('-__v')
      .lean();

    if (!user) {
      throw new AppError('User not found', 404, 'USER_NOT_FOUND');
    }

    // Check if current user is following
    let isFollowing = false;
    if (req.user?.userId) {
      isFollowing = await followService.isFollowing(req.user.userId, userId);
    }

    res.json({
      success: true,
      data: {
        user,
        isFollowing,
      },
    });
  });
}

export const userController = new UserController();
