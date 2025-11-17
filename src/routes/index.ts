import { Router } from 'express';
import { timelineController } from '../controllers/TimelineController';
import { postController } from '../controllers/PostController';
import { userController } from '../controllers/UserController';
import { authenticate, optionalAuth } from '../middlewares/auth';
import { rateLimiter, strictRateLimiter } from '../middlewares/rateLimiter';

const router = Router();

// Health check
router.get('/health', (req, res) => {
  res.json({
    success: true,
    status: 'healthy',
    timestamp: new Date().toISOString(),
  });
});

// User routes
router.post('/users/register', strictRateLimiter, userController.register);
router.post('/users/login', strictRateLimiter, userController.login);
router.post('/users/:userId/follow', authenticate, rateLimiter(), userController.followUser);
router.delete('/users/:userId/follow', authenticate, rateLimiter(), userController.unfollowUser);
router.get('/users/:userId/followers', optionalAuth, userController.getFollowers);
router.get('/users/:userId/following', optionalAuth, userController.getFollowing);
router.get('/users/:userId', optionalAuth, userController.getProfile);
router.get('/users/:userId/posts', optionalAuth, postController.getUserPosts);

// Timeline routes
router.get('/timeline', authenticate, rateLimiter(), timelineController.getTimeline);
router.get('/timeline/trending', optionalAuth, timelineController.getTrending);
router.post('/timeline/refresh', authenticate, strictRateLimiter, timelineController.refreshTimeline);

// Post routes
router.post('/posts', authenticate, rateLimiter(), postController.createPost);
router.get('/posts/:postId', optionalAuth, postController.getPost);
router.post('/posts/:postId/like', authenticate, rateLimiter(), postController.likePost);
router.delete('/posts/:postId', authenticate, rateLimiter(), postController.deletePost);

export default router;
