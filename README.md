# Timeline Feed System

高性能社交媒体 Timeline Feed 系统，支持百万级用户的实时内容分发。

## 功能特性

- 用户关注关系管理（MongoDB + Redis）
- Feed Fanout 推送系统（Kafka）
- Timeline 缓存（Redis Sorted Set）
- 分页查询（Cursor-based）
- Rate Limiting 限流保护
- 热点内容缓存优化
- Twitter 数据集集成

## 技术栈

- **Backend**: Node.js, TypeScript, Express
- **Database**: MongoDB, Redis
- **Message Queue**: Kafka
- **Container**: Docker, Docker Compose

## 快速开始

### 前置要求

- Node.js >= 18.0.0
- Docker & Docker Compose
- npm >= 9.0.0

### 安装

```bash
# 克隆项目
git clone <repository-url>
cd Timeline-Feed

# 安装依赖
npm install

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件，配置你的环境变量

# 启动基础设施（MongoDB, Redis, Kafka）
npm run docker:up

# 运行数据迁移
npm run migrate

# （可选）导入测试数据
npm run seed
```

### 开发

```bash
# 开发模式（热重载）
npm run dev

# 构建
npm run build

# 生产模式
npm start

# 运行测试
npm test

# 代码检查
npm run lint

# 格式化代码
npm run format
```

## API 文档

### Timeline API

#### 获取用户 Timeline

```http
GET /api/v1/timeline?limit=20&cursor=<cursor>
Authorization: Bearer <token>
```

**Response:**
```json
{
  "success": true,
  "data": {
    "posts": [
      {
        "id": "post123",
        "userId": "user456",
        "content": "Hello World",
        "createdAt": "2024-01-01T00:00:00Z",
        "likes": 10,
        "retweets": 5
      }
    ],
    "pagination": {
      "nextCursor": "eyJ0aW1lc3RhbXAiOjE3MDAwMDAwMDB9",
      "hasMore": true
    }
  }
}
```

### Post API

#### 创建帖子

```http
POST /api/v1/posts
Authorization: Bearer <token>
Content-Type: application/json

{
  "content": "Hello World",
  "mediaUrls": []
}
```

### User API

#### 关注用户

```http
POST /api/v1/users/:userId/follow
Authorization: Bearer <token>
```

#### 取消关注

```http
DELETE /api/v1/users/:userId/follow
Authorization: Bearer <token>
```

## 项目结构

```
Timeline-Feed/
├── src/
│   ├── config/           # 配置文件
│   ├── models/           # 数据模型
│   ├── services/         # 业务逻辑
│   ├── controllers/      # 控制器
│   ├── routes/           # 路由
│   ├── middlewares/      # 中间件
│   ├── workers/          # Kafka 消费者
│   ├── utils/            # 工具函数
│   ├── types/            # TypeScript 类型
│   └── index.ts          # 入口文件
├── scripts/              # 脚本
├── tests/                # 测试
├── docker-compose.yml    # Docker 配置
└── ARCHITECTURE.md       # 架构文档
```

## 架构设计

详见 [ARCHITECTURE.md](./ARCHITECTURE.md)

### 核心组件

1. **Follow Graph Service**: 用户关注关系管理
2. **Post Service**: 帖子创建和管理
3. **Timeline Service**: Timeline 查询服务
4. **Fanout Worker**: Feed 分发工作者
5. **Cache Service**: Redis 缓存层
6. **Rate Limiter**: 限流中间件

### Fanout 模式

- **Push Model**: 粉丝 < 10k，实时推送
- **Pull Model**: 粉丝 >= 10k，按需拉取

## 性能指标

- Timeline 查询延迟: < 50ms (P95)
- 发帖延迟: < 100ms
- Fanout 延迟: < 500ms
- QPS: 10,000+

## 数据集

支持导入 Twitter Kaggle 数据集进行测试：

```bash
# 下载数据集到 data/ 目录
# 运行导入脚本
npm run seed -- --dataset=twitter --file=data/tweets.csv
```

## 监控

```bash
# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f

# 监控 Kafka
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## 故障排查

### Kafka 连接失败
```bash
# 检查 Kafka 状态
docker-compose logs kafka

# 重启 Kafka
docker-compose restart kafka
```

### Redis 连接失败
```bash
# 检查 Redis 状态
docker-compose exec redis redis-cli ping

# 清空 Redis
docker-compose exec redis redis-cli FLUSHALL
```

## License

MIT
