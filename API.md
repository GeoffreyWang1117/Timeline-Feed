# Timeline Feed API Documentation

Base URL: `http://localhost:3000/api/v1`

## Authentication

Most endpoints require authentication using JWT Bearer tokens.

```http
Authorization: Bearer <your-jwt-token>
```

## Rate Limiting

API implements rate limiting:
- Global: 1000 req/min per IP
- Authenticated: 100 req/min per user
- Strict operations: 10 req/min

Rate limit headers:
```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1234567890
```

## Endpoints

### User Management

#### Register User

```http
POST /users/register
Content-Type: application/json

{
  "username": "johndoe",
  "email": "john@example.com",
  "password": "password123",
  "displayName": "John Doe"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "user": {
      "userId": "uuid",
      "username": "johndoe",
      "displayName": "John Doe",
      "email": "john@example.com"
    },
    "token": "jwt-token"
  }
}
```

#### Get User Profile

```http
GET /users/:userId
```

**Response:**
```json
{
  "success": true,
  "data": {
    "user": {
      "userId": "uuid",
      "username": "johndoe",
      "displayName": "John Doe",
      "bio": "Software engineer",
      "followerCount": 100,
      "followingCount": 50,
      "postCount": 25,
      "isVerified": false,
      "isCelebrity": false
    },
    "isFollowing": false
  }
}
```

#### Follow User

```http
POST /users/:userId/follow
Authorization: Bearer <token>
```

**Response:**
```json
{
  "success": true,
  "message": "User followed"
}
```

#### Unfollow User

```http
DELETE /users/:userId/follow
Authorization: Bearer <token>
```

#### Get Followers

```http
GET /users/:userId/followers?limit=100&offset=0
```

**Response:**
```json
{
  "success": true,
  "data": {
    "followers": [
      {
        "userId": "uuid",
        "username": "janedoe",
        "displayName": "Jane Doe",
        "avatarUrl": "https://...",
        "isVerified": false
      }
    ],
    "pagination": {
      "limit": 100,
      "offset": 0,
      "hasMore": true
    }
  }
}
```

#### Get Following

```http
GET /users/:userId/following?limit=100&offset=0
```

### Timeline

#### Get Home Timeline

```http
GET /timeline?limit=20&cursor=<optional-cursor>
Authorization: Bearer <token>
```

**Query Parameters:**
- `limit` (optional): Number of posts (1-100, default: 20)
- `cursor` (optional): Pagination cursor for next page

**Response:**
```json
{
  "success": true,
  "data": {
    "posts": [
      {
        "postId": "uuid",
        "userId": "uuid",
        "username": "johndoe",
        "displayName": "John Doe",
        "avatarUrl": "https://...",
        "isVerified": false,
        "content": "Hello World!",
        "mediaUrls": [],
        "hashtags": ["hello"],
        "mentions": [],
        "likeCount": 10,
        "retweetCount": 5,
        "replyCount": 2,
        "viewCount": 100,
        "isHot": false,
        "createdAt": "2024-01-01T00:00:00Z"
      }
    ],
    "pagination": {
      "nextCursor": "eyJ0aW1lc3RhbXAiOjE3MDAwMDAwMDB9",
      "hasMore": true,
      "limit": 20
    }
  }
}
```

#### Get Trending Timeline

```http
GET /timeline/trending?limit=20
```

**Response:** Same as home timeline

#### Refresh Timeline Cache

```http
POST /timeline/refresh
Authorization: Bearer <token>
```

**Response:**
```json
{
  "success": true,
  "message": "Timeline cache refreshed"
}
```

### Posts

#### Create Post

```http
POST /posts
Authorization: Bearer <token>
Content-Type: application/json

{
  "content": "Hello World! #greeting @janedoe",
  "mediaUrls": ["https://example.com/image.jpg"]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "post": {
      "postId": "uuid",
      "userId": "uuid",
      "content": "Hello World!",
      "hashtags": ["greeting"],
      "mentions": ["janedoe"],
      "createdAt": "2024-01-01T00:00:00Z"
    }
  }
}
```

#### Get Post

```http
GET /posts/:postId
```

**Response:**
```json
{
  "success": true,
  "data": {
    "post": {
      "postId": "uuid",
      "userId": "uuid",
      "username": "johndoe",
      "displayName": "John Doe",
      "content": "Hello World!",
      "likeCount": 10,
      "createdAt": "2024-01-01T00:00:00Z"
    }
  }
}
```

#### Like Post

```http
POST /posts/:postId/like
Authorization: Bearer <token>
```

#### Delete Post

```http
DELETE /posts/:postId
Authorization: Bearer <token>
```

#### Get User Posts

```http
GET /users/:userId/posts?limit=20&offset=0
```

### Health Check

```http
GET /health
```

**Response:**
```json
{
  "success": true,
  "status": "healthy",
  "timestamp": "2024-01-01T00:00:00Z"
}
```

## Error Responses

### 400 Bad Request
```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid input data"
  }
}
```

### 401 Unauthorized
```json
{
  "success": false,
  "error": {
    "code": "UNAUTHORIZED",
    "message": "No token provided"
  }
}
```

### 404 Not Found
```json
{
  "success": false,
  "error": {
    "code": "NOT_FOUND",
    "message": "Resource not found"
  }
}
```

### 429 Too Many Requests
```json
{
  "success": false,
  "error": {
    "code": "RATE_LIMIT_EXCEEDED",
    "message": "Too many requests. Please try again later."
  },
  "retryAfter": 30
}
```

### 500 Internal Server Error
```json
{
  "success": false,
  "error": {
    "code": "INTERNAL_SERVER_ERROR",
    "message": "Something went wrong"
  }
}
```

## Cursor-based Pagination

For timeline endpoints, we use cursor-based pagination for better performance:

1. First request: `GET /timeline?limit=20`
2. Subsequent requests: `GET /timeline?limit=20&cursor=<nextCursor>`

The cursor is a base64-encoded string containing:
```json
{
  "timestamp": 1700000000,
  "postId": "uuid"
}
```

## Performance Tips

1. Use cursor-based pagination for timelines
2. Cache user tokens on the client
3. Batch operations when possible
4. Monitor rate limit headers
5. Use trending timeline for hot content

## WebSocket Support (Future)

Real-time updates via WebSocket will be available at:
```
ws://localhost:3000/ws
```

## SDK Examples

### JavaScript/Node.js

```javascript
const axios = require('axios');

const client = axios.create({
  baseURL: 'http://localhost:3000/api/v1',
  headers: {
    'Authorization': 'Bearer <token>'
  }
});

// Get timeline
const timeline = await client.get('/timeline?limit=20');

// Create post
const post = await client.post('/posts', {
  content: 'Hello World!'
});
```

### cURL

```bash
# Register
curl -X POST http://localhost:3000/api/v1/users/register \
  -H "Content-Type: application/json" \
  -d '{"username":"johndoe","email":"john@example.com","password":"pass123","displayName":"John Doe"}'

# Get timeline
curl -X GET http://localhost:3000/api/v1/timeline \
  -H "Authorization: Bearer <token>"

# Create post
curl -X POST http://localhost:3000/api/v1/posts \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"content":"Hello World!"}'
```
