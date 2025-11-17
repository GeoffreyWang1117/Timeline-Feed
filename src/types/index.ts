// Common types and interfaces

export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
  };
  pagination?: PaginationInfo;
}

export interface PaginationInfo {
  nextCursor?: string;
  prevCursor?: string;
  hasMore: boolean;
  limit: number;
}

export interface TimelinePost {
  postId: string;
  userId: string;
  username: string;
  displayName: string;
  avatarUrl?: string;
  isVerified: boolean;
  content: string;
  mediaUrls?: string[];
  hashtags?: string[];
  mentions?: string[];
  likeCount: number;
  retweetCount: number;
  replyCount: number;
  viewCount: number;
  isHot: boolean;
  createdAt: Date;
}

export interface CursorData {
  timestamp: number;
  postId: string;
}

export interface FanoutMessage {
  postId: string;
  userId: string;
  content: string;
  createdAt: number;
  isCelebrity: boolean;
}

export interface RateLimitInfo {
  limit: number;
  remaining: number;
  reset: number;
}

export enum FanoutStrategy {
  PUSH = 'push',
  PULL = 'pull',
  HYBRID = 'hybrid',
}

export enum CacheStrategy {
  LRU = 'lru',
  LFU = 'lfu',
  TTL = 'ttl',
}

export interface HotContentMetrics {
  postId: string;
  views: number;
  uniqueVisitors: number;
  score: number;
}
