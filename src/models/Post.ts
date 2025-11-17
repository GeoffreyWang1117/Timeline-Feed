import mongoose, { Document, Schema } from 'mongoose';

export interface IPost extends Document {
  postId: string;
  userId: string;
  content: string;
  mediaUrls?: string[];
  hashtags?: string[];
  mentions?: string[];
  likeCount: number;
  retweetCount: number;
  replyCount: number;
  viewCount: number;
  isHot: boolean; // Marked as hot content
  createdAt: Date;
  updatedAt: Date;
}

const postSchema = new Schema<IPost>(
  {
    postId: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    userId: {
      type: String,
      required: true,
      index: true,
      ref: 'User',
    },
    content: {
      type: String,
      required: true,
      maxlength: 280, // Twitter-style limit
    },
    mediaUrls: [{
      type: String,
    }],
    hashtags: [{
      type: String,
      lowercase: true,
      trim: true,
    }],
    mentions: [{
      type: String,
      ref: 'User',
    }],
    likeCount: {
      type: Number,
      default: 0,
      min: 0,
    },
    retweetCount: {
      type: Number,
      default: 0,
      min: 0,
    },
    replyCount: {
      type: Number,
      default: 0,
      min: 0,
    },
    viewCount: {
      type: Number,
      default: 0,
      min: 0,
    },
    isHot: {
      type: Boolean,
      default: false,
      index: true,
    },
  },
  {
    timestamps: true,
  }
);

// Indexes
postSchema.index({ userId: 1, createdAt: -1 });
postSchema.index({ createdAt: -1 });
postSchema.index({ hashtags: 1 });
postSchema.index({ viewCount: -1, createdAt: -1 }); // For trending
postSchema.index({ isHot: 1, createdAt: -1 }); // For hot content

export const Post = mongoose.model<IPost>('Post', postSchema);
