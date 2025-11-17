// MongoDB initialization script for Docker

db = db.getSiblingDB('timeline_feed');

// Create collections
db.createCollection('users');
db.createCollection('posts');
db.createCollection('follows');

// Create indexes

// Users
db.users.createIndex({ userId: 1 }, { unique: true });
db.users.createIndex({ username: 1 }, { unique: true });
db.users.createIndex({ email: 1 }, { unique: true });
db.users.createIndex({ isCelebrity: 1 });
db.users.createIndex({ followerCount: -1 });

// Posts
db.posts.createIndex({ postId: 1 }, { unique: true });
db.posts.createIndex({ userId: 1, createdAt: -1 });
db.posts.createIndex({ createdAt: -1 });
db.posts.createIndex({ hashtags: 1 });
db.posts.createIndex({ isHot: 1, createdAt: -1 });
db.posts.createIndex({ viewCount: -1, createdAt: -1 });

// Follows
db.follows.createIndex({ followerId: 1, followingId: 1 }, { unique: true });
db.follows.createIndex({ followingId: 1, createdAt: -1 });
db.follows.createIndex({ followerId: 1, createdAt: -1 });

print('MongoDB initialized successfully');
