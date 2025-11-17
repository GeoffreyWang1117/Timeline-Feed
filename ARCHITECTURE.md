# Timeline Feed System - 架构设计

## 1. 系统概述

这是一个高性能的社交媒体 Timeline Feed 系统，支持百万级用户的实时内容分发。

### 核心特性
- 用户关注关系管理
- Feed 内容实时分发（Push + Pull 混合模式）
- 高性能缓存层
- 分布式消息队列
- Rate Limiting 防护
- 热点内容优化

## 2. 技术栈

### 后端服务
- **Node.js + TypeScript**: 主服务
- **Express**: Web 框架
- **Kafka**: 消息队列（Feed Fanout）
- **Redis**: 缓存层 + Timeline 存储
- **MongoDB**: 持久化存储

### 基础设施
- **Docker + Docker Compose**: 容器化
- **Nginx**: 反向代理 + 负载均衡

## 3. 系统架构

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────┐
│         API Gateway (Nginx)         │
│       Rate Limiting Middleware      │
└──────────────┬──────────────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
┌────────────┐   ┌────────────┐
│ Timeline   │   │  Post      │
│ Service    │   │  Service   │
└─────┬──────┘   └─────┬──────┘
      │                │
      │                ▼
      │         ┌─────────────┐
      │         │    Kafka    │
      │         │  (Fanout)   │
      │         └──────┬──────┘
      │                │
      │                ▼
      │         ┌─────────────┐
      │         │   Fanout    │
      │         │   Worker    │
      │         └──────┬──────┘
      │                │
      └────────┬───────┘
               ▼
        ┌─────────────┐
        │    Redis    │
        │ (Timeline)  │
        │ Sorted Set  │
        └─────────────┘
               │
               ▼
        ┌─────────────┐
        │   MongoDB   │
        │ (Followers) │
        │   (Posts)   │
        └─────────────┘
```

## 4. 核心组件设计

### 4.1 用户关系图（Follow Graph）

**存储方案：**
- **MongoDB**: 持久化存储
  - `followers` collection: 用户关注关系
  - `users` collection: 用户信息

- **Redis**: 热数据缓存
  - Key: `follower:{userId}` → Set of follower IDs
  - Key: `following:{userId}` → Set of following IDs
  - TTL: 1小时

**数据结构：**
```javascript
// MongoDB
{
  _id: ObjectId,
  userId: string,
  followerId: string,
  createdAt: Date,
  index: [userId, followerId] // 复合索引
}

// Redis
SADD follower:user123 user456 user789
SADD following:user456 user123
```

### 4.2 Feed Fanout 模式

采用 **Push + Pull 混合模式**：

**Push Model（用于普通用户）**
- 当用户发帖时，立即推送到其粉丝的 Timeline
- 适用于：粉丝数 < 10,000 的用户
- 优点：读取快速
- 缺点：写入成本高

**Pull Model（用于大V）**
- 粉丝实时拉取大V的最新帖子
- 适用于：粉丝数 >= 10,000 的用户
- 优点：写入快速
- 缺点：读取时需要聚合

**实现流程：**
```
1. 用户发帖 → Post Service
2. Post Service → Kafka Topic: "new_post"
3. Fanout Worker 消费消息
4. 判断用户类型：
   - 普通用户：Push to followers' timeline
   - 大V：标记为 pull，存储到单独的 list
5. 更新 Redis Timeline Cache
```

### 4.3 Redis Timeline Cache

**数据结构：Sorted Set**
```redis
# 用户的 timeline
ZADD timeline:user123 1700000000 post456
ZADD timeline:user123 1700000100 post789

# Score = timestamp (毫秒)
# Value = postId

# 读取 timeline（分页）
ZREVRANGE timeline:user123 0 19 WITHSCORES  # 前20条

# 缓存策略
- TTL: 30 分钟
- Max size: 1000 条/用户
- 使用 ZREMRANGEBYRANK 清理旧数据
```

**热点内容缓存：**
```redis
# 热门帖子详情
SETEX post:456 3600 "{json_content}"

# 热门帖子计数器
INCR post:456:views
EXPIRE post:456:views 86400
```

### 4.4 Timeline API 设计

**Endpoint：**
```
GET /api/v1/timeline
```

**分页方案：**

**方案1: Offset-based（简单场景）**
```javascript
GET /api/v1/timeline?limit=20&offset=0
{
  "posts": [...],
  "pagination": {
    "offset": 0,
    "limit": 20,
    "total": 1000,
    "hasMore": true
  }
}
```

**方案2: Cursor-based（推荐）**
```javascript
GET /api/v1/timeline?limit=20&cursor=eyJ0aW1lc3RhbXAiOjE3MDAwMDAwMDB9

{
  "posts": [...],
  "pagination": {
    "nextCursor": "eyJ0aW1lc3RhbXAiOjE2OTk5OTk5ODB9",
    "hasMore": true
  }
}

// Cursor = Base64({ timestamp: lastPostTimestamp, postId: lastPostId })
```

### 4.5 Rate Limiting

**策略：Token Bucket 算法**

**限流层级：**
1. **全局限流**: 1000 req/s
2. **用户限流**: 10 req/s per user
3. **API限流**:
   - 读操作: 100 req/min
   - 写操作: 20 req/min

**实现：Redis**
```javascript
// 使用 Redis + Lua 脚本
const key = `ratelimit:${userId}:${endpoint}`;
const limit = 10;
const window = 60; // seconds

// Lua script for atomic check
EVAL `
  local current = redis.call('INCR', KEYS[1])
  if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
  end
  if current > tonumber(ARGV[2]) then
    return 0
  end
  return 1
` 1 key window limit
```

### 4.6 热点内容缓存

**三级缓存架构：**

```
L1: Application Memory Cache (LRU, 100MB)
 │
 └─▶ L2: Redis Cache (10GB)
      │
      └─▶ L3: MongoDB (Persistent)
```

**热点检测：**
```javascript
// 使用 Redis HyperLogLog 统计 UV
PFADD post:456:visitors user123
PFCOUNT post:456:visitors  // 获取独立访问数

// 使用 Sorted Set 维护热榜
ZINCRBY trending:posts 1 post456
ZREVRANGE trending:posts 0 99  // Top 100
```

## 5. 数据集成方案

### 5.1 Twitter Dataset
- 来源：Kaggle Twitter Dataset
- 格式：CSV/JSON
- 字段：tweet_id, user_id, text, created_at, retweet_count, like_count

### 5.2 数据导入流程
```bash
1. 下载数据集
2. 清洗数据（去重、格式化）
3. 批量导入 MongoDB
4. 生成测试用户关系图
5. 预热 Redis 缓存
```

## 6. 性能指标

### 目标
- **Timeline 查询延迟**: < 50ms (P95)
- **发帖延迟**: < 100ms
- **Fanout 延迟**: < 500ms
- **QPS**: 10,000+
- **用户容量**: 1M+
- **帖子容量**: 10M+

### 优化策略
1. **读写分离**: MongoDB Replica Set
2. **缓存预热**: 定期刷新热点数据
3. **异步处理**: Kafka 消息队列
4. **数据分片**: 按 userId 分片
5. **CDN**: 静态资源分发

## 7. 监控与可观测性

### 指标
- API 响应时间
- Kafka 消费延迟
- Redis 命中率
- MongoDB 慢查询
- 错误率

### 工具
- Prometheus + Grafana
- ELK Stack (日志)
- Jaeger (分布式追踪)

## 8. 部署架构

```yaml
services:
  - api-gateway (Nginx)
  - timeline-service (3 replicas)
  - post-service (2 replicas)
  - fanout-worker (5 replicas)
  - kafka (3 brokers)
  - redis (1 master + 2 replicas)
  - mongodb (1 primary + 2 secondaries)
```

## 9. 后续扩展

1. **个性化推荐**: 集成机器学习算法
2. **实时通知**: WebSocket 推送
3. **图片/视频**: 对象存储（S3/OSS）
4. **搜索功能**: Elasticsearch
5. **多地域部署**: 数据中心跨区
