import mongoose, { Document, Schema } from 'mongoose';

export interface IPassword extends Document {
  userId: string;
  passwordHash: string;
  salt: string;
  failedLoginAttempts: number;
  lockedUntil?: Date;
  lastPasswordChange: Date;
  createdAt: Date;
  updatedAt: Date;
}

const passwordSchema = new Schema<IPassword>(
  {
    userId: {
      type: String,
      required: true,
      unique: true,
      index: true,
      ref: 'User',
    },
    passwordHash: {
      type: String,
      required: true,
    },
    salt: {
      type: String,
      required: true,
    },
    failedLoginAttempts: {
      type: Number,
      default: 0,
      min: 0,
    },
    lockedUntil: {
      type: Date,
    },
    lastPasswordChange: {
      type: Date,
      default: Date.now,
    },
  },
  {
    timestamps: true,
  }
);

// Indexes
passwordSchema.index({ userId: 1 });
passwordSchema.index({ lockedUntil: 1 }, { sparse: true });

// Check if account is locked
passwordSchema.methods.isLocked = function (): boolean {
  return !!(this.lockedUntil && this.lockedUntil > new Date());
};

// Increment failed login attempts
passwordSchema.methods.incLoginAttempts = async function (): Promise<void> {
  // Reset attempts if lock has expired
  if (this.lockedUntil && this.lockedUntil < new Date()) {
    await this.updateOne({
      $set: { failedLoginAttempts: 1 },
      $unset: { lockedUntil: 1 },
    });
    return;
  }

  // Increment attempts
  const updates: any = { $inc: { failedLoginAttempts: 1 } };

  // Lock account after 5 failed attempts (30 minutes)
  const maxAttempts = 5;
  const lockTime = 30 * 60 * 1000; // 30 minutes

  if (this.failedLoginAttempts + 1 >= maxAttempts && !this.isLocked()) {
    updates.$set = { lockedUntil: new Date(Date.now() + lockTime) };
  }

  await this.updateOne(updates);
};

// Reset failed login attempts on successful login
passwordSchema.methods.resetLoginAttempts = async function (): Promise<void> {
  await this.updateOne({
    $set: { failedLoginAttempts: 0 },
    $unset: { lockedUntil: 1 },
  });
};

export const Password = mongoose.model<IPassword>('Password', passwordSchema);
