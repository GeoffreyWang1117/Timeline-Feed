import mongoose, { Document, Schema } from 'mongoose';

export interface IFollow extends Document {
  followerId: string; // User who follows
  followingId: string; // User being followed
  createdAt: Date;
}

const followSchema = new Schema<IFollow>(
  {
    followerId: {
      type: String,
      required: true,
      index: true,
      ref: 'User',
    },
    followingId: {
      type: String,
      required: true,
      index: true,
      ref: 'User',
    },
  },
  {
    timestamps: { createdAt: true, updatedAt: false },
  }
);

// Compound indexes for efficient queries
followSchema.index({ followerId: 1, followingId: 1 }, { unique: true });
followSchema.index({ followingId: 1, createdAt: -1 });
followSchema.index({ followerId: 1, createdAt: -1 });

// Prevent self-follow
followSchema.pre('save', function (next) {
  if (this.followerId === this.followingId) {
    next(new Error('Users cannot follow themselves'));
  } else {
    next();
  }
});

export const Follow = mongoose.model<IFollow>('Follow', followSchema);
