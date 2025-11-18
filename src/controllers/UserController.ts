import { Response } from 'express';
import { followService } from '../services/FollowService';
import { User } from '../models/User';
import { Password } from '../models/Password';
import { AuthRequest, generateToken } from '../middlewares/auth';
import { AppError, asyncHandler } from '../middlewares/errorHandler';
import { v4 as uuidv4 } from 'uuid';
import bcrypt from 'bcryptjs';
import crypto from 'crypto';
import Joi from 'joi';
import { logger } from '../utils/logger';

const registerSchema = Joi.object({
  username: Joi.string().required().min(3).max(30).lowercase().pattern(/^[a-z0-9_]+$/),
  email: Joi.string().required().email(),
  password: Joi.string()
    .required()
    .min(8)
    .pattern(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]/)
    .messages({
      'string.pattern.base': 'Password must contain at least one uppercase letter, one lowercase letter, one number, and one special character',
      'string.min': 'Password must be at least 8 characters long',
    }),
  displayName: Joi.string().required().max(50),
});

const loginSchema = Joi.object({
  email: Joi.string().required().email(),
  password: Joi.string().required(),
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

    // Generate salt and hash password
    const salt = crypto.randomBytes(16).toString('hex');
    const passwordHash = await bcrypt.hash(password + salt, 12);

    // Create user
    const userId = uuidv4();
    const user = await User.create({
      userId,
      username,
      email,
      displayName,
    });

    // Store password securely
    await Password.create({
      userId,
      passwordHash,
      salt,
    });

    logger.info(`User registered: ${userId} (${username})`);

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
   * POST /api/v1/users/login
   * Login user
   */
  login = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { error, value } = loginSchema.validate(req.body);
    if (error) {
      throw new AppError(error.details[0].message, 400, 'VALIDATION_ERROR');
    }

    const { email, password } = value;

    // Find user
    const user = await User.findOne({ email });
    if (!user) {
      throw new AppError('Invalid credentials', 401, 'INVALID_CREDENTIALS');
    }

    // Get password
    const passwordDoc = await Password.findOne({ userId: user.userId });
    if (!passwordDoc) {
      logger.error(`Password not found for user: ${user.userId}`);
      throw new AppError('Invalid credentials', 401, 'INVALID_CREDENTIALS');
    }

    // Check if account is locked
    if (passwordDoc.isLocked()) {
      const lockTimeRemaining = Math.ceil(
        (passwordDoc.lockedUntil!.getTime() - Date.now()) / 1000 / 60
      );
      throw new AppError(
        `Account is locked. Try again in ${lockTimeRemaining} minutes`,
        423,
        'ACCOUNT_LOCKED'
      );
    }

    // Verify password
    const isValid = await bcrypt.compare(
      password + passwordDoc.salt,
      passwordDoc.passwordHash
    );

    if (!isValid) {
      // Increment failed attempts
      await passwordDoc.incLoginAttempts();

      const remainingAttempts = 5 - (passwordDoc.failedLoginAttempts + 1);
      if (remainingAttempts > 0) {
        throw new AppError(
          `Invalid credentials. ${remainingAttempts} attempts remaining`,
          401,
          'INVALID_CREDENTIALS'
        );
      } else {
        throw new AppError(
          'Invalid credentials. Account locked for 30 minutes',
          423,
          'ACCOUNT_LOCKED'
        );
      }
    }

    // Reset failed attempts on successful login
    await passwordDoc.resetLoginAttempts();

    logger.info(`User logged in: ${user.userId} (${user.username})`);

    // Generate token
    const token = generateToken(user.userId, user.username);

    res.json({
      success: true,
      data: {
        user: {
          userId: user.userId,
          username: user.username,
          displayName: user.displayName,
          email: user.email,
          followerCount: user.followerCount,
          followingCount: user.followingCount,
          postCount: user.postCount,
          isVerified: user.isVerified,
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
   * Get user's followers (optimized with aggregation)
   */
  getFollowers = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { userId } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);
    const offset = parseInt(req.query.offset as string) || 0;

    // Use optimized aggregation query (1 query instead of 2)
    const followers = await followService.getFollowersWithDetails(userId, limit + 1, offset);

    const hasMore = followers.length > limit;
    const results = hasMore ? followers.slice(0, limit) : followers;

    res.json({
      success: true,
      data: {
        followers: results,
        pagination: {
          limit,
          offset,
          hasMore,
        },
      },
    });
  });

  /**
   * GET /api/v1/users/:userId/following
   * Get users that a user is following (optimized with aggregation)
   */
  getFollowing = asyncHandler(async (req: AuthRequest, res: Response) => {
    const { userId } = req.params;
    const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);
    const offset = parseInt(req.query.offset as string) || 0;

    // Use optimized aggregation query (1 query instead of 2)
    const following = await followService.getFollowingWithDetails(userId, limit + 1, offset);

    const hasMore = following.length > limit;
    const results = hasMore ? following.slice(0, limit) : following;

    res.json({
      success: true,
      data: {
        following: results,
        pagination: {
          limit,
          offset,
          hasMore,
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
