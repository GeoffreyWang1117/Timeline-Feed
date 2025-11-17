import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import config from '../config';
import { AppError } from './errorHandler';

export interface AuthRequest extends Request {
  user?: {
    userId: string;
    username: string;
  };
}

/**
 * Simple JWT authentication middleware
 * In production, integrate with your actual auth system
 */
export const authenticate = (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): void => {
  try {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      throw new AppError('No token provided', 401, 'UNAUTHORIZED');
    }

    const token = authHeader.split(' ')[1];

    try {
      const decoded = jwt.verify(token, config.jwt.secret) as any;
      req.user = {
        userId: decoded.userId,
        username: decoded.username,
      };
      next();
    } catch (error) {
      throw new AppError('Invalid token', 401, 'INVALID_TOKEN');
    }
  } catch (error) {
    next(error);
  }
};

/**
 * Optional authentication - don't fail if no token
 */
export const optionalAuth = (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): void => {
  try {
    const authHeader = req.headers.authorization;

    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.split(' ')[1];

      try {
        const decoded = jwt.verify(token, config.jwt.secret) as any;
        req.user = {
          userId: decoded.userId,
          username: decoded.username,
        };
      } catch (error) {
        // Ignore invalid token
      }
    }

    next();
  } catch (error) {
    next(error);
  }
};

/**
 * Generate JWT token (helper function)
 */
export const generateToken = (userId: string, username: string): string => {
  return jwt.sign(
    { userId, username },
    config.jwt.secret,
    { expiresIn: config.jwt.expiresIn }
  );
};
