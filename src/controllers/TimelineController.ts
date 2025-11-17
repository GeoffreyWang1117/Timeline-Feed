import { Response } from 'express';
import { timelineService } from '../services/TimelineService';
import { AuthRequest } from '../middlewares/auth';
import { AppError, asyncHandler } from '../middlewares/errorHandler';
import { logger } from '../utils/logger';

export class TimelineController {
  /**
   * GET /api/v1/timeline
   * Get user's home timeline
   */
  getTimeline = asyncHandler(async (req: AuthRequest, res: Response) => {
    const userId = req.user?.userId;
    if (!userId) {
      throw new AppError('User not authenticated', 401, 'UNAUTHORIZED');
    }

    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
    const cursor = req.query.cursor as string | undefined;

    const result = await timelineService.getTimeline(userId, limit, cursor);

    res.json({
      success: true,
      data: result,
    });
  });

  /**
   * GET /api/v1/timeline/trending
   * Get trending/hot timeline
   */
  getTrending = asyncHandler(async (req: AuthRequest, res: Response) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
    const cursor = req.query.cursor as string | undefined;

    const result = await timelineService.getTrendingTimeline(limit, cursor);

    res.json({
      success: true,
      data: result,
    });
  });

  /**
   * POST /api/v1/timeline/refresh
   * Refresh user's timeline cache
   */
  refreshTimeline = asyncHandler(async (req: AuthRequest, res: Response) => {
    const userId = req.user?.userId;
    if (!userId) {
      throw new AppError('User not authenticated', 401, 'UNAUTHORIZED');
    }

    await timelineService.refreshTimelineCache(userId);

    res.json({
      success: true,
      message: 'Timeline cache refreshed',
    });
  });
}

export const timelineController = new TimelineController();
