# Deployment Guide

## Prerequisites

- Node.js >= 18.0.0
- Docker & Docker Compose
- MongoDB 7.0+
- Redis 7.0+
- Kafka 3.5+

## Local Development

### 1. Install Dependencies

```bash
npm install
```

### 2. Environment Setup

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 3. Start Infrastructure

```bash
# Start MongoDB, Redis, Kafka
npm run docker:up

# Wait for services to be healthy (30-60 seconds)
docker-compose ps
```

### 4. Initialize Database

```bash
# Run MongoDB migrations
npm run migrate

# (Optional) Generate test data
npm run seed follow-graph 100 20
```

### 5. Start Application

```bash
# Development mode (hot reload)
npm run dev

# Production build
npm run build
npm start
```

### 6. Start Fanout Worker

In a separate terminal:

```bash
npm run dev -- src/workers/fanout-worker.ts
```

## Production Deployment

### Docker Deployment

#### Build Images

```bash
# Build application image
docker build -t timeline-feed:latest -f Dockerfile .

# Build worker image
docker build -t timeline-feed-worker:latest -f Dockerfile.worker .
```

#### Deploy with Docker Compose

```bash
# Uncomment app and fanout-worker services in docker-compose.yml
# Then start all services
docker-compose up -d

# Check logs
docker-compose logs -f app
docker-compose logs -f fanout-worker
```

### Kubernetes Deployment

#### Prerequisites

- Kubernetes cluster (GKE, EKS, AKS)
- kubectl configured
- Helm 3+

#### Deploy MongoDB (using Helm)

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami

helm install mongodb bitnami/mongodb \
  --set auth.enabled=false \
  --set architecture=replicaset \
  --set replicaCount=3
```

#### Deploy Redis

```bash
helm install redis bitnami/redis \
  --set auth.enabled=false \
  --set master.persistence.enabled=true \
  --set replica.replicaCount=2
```

#### Deploy Kafka

```bash
helm install kafka bitnami/kafka \
  --set replicaCount=3 \
  --set zookeeper.replicaCount=3
```

#### Deploy Application

Create `k8s/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: timeline-feed
spec:
  replicas: 3
  selector:
    matchLabels:
      app: timeline-feed
  template:
    metadata:
      labels:
        app: timeline-feed
    spec:
      containers:
      - name: timeline-feed
        image: timeline-feed:latest
        ports:
        - containerPort: 3000
        env:
        - name: MONGODB_URI
          value: "mongodb://mongodb:27017/timeline_feed"
        - name: REDIS_HOST
          value: "redis-master"
        - name: KAFKA_BROKERS
          value: "kafka:9092"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: timeline-feed
spec:
  selector:
    app: timeline-feed
  ports:
  - port: 80
    targetPort: 3000
  type: LoadBalancer
```

Deploy:

```bash
kubectl apply -f k8s/deployment.yaml
```

#### Deploy Fanout Worker

Create `k8s/worker.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fanout-worker
spec:
  replicas: 5
  selector:
    matchLabels:
      app: fanout-worker
  template:
    metadata:
      labels:
        app: fanout-worker
    spec:
      containers:
      - name: fanout-worker
        image: timeline-feed-worker:latest
        env:
        - name: MONGODB_URI
          value: "mongodb://mongodb:27017/timeline_feed"
        - name: REDIS_HOST
          value: "redis-master"
        - name: KAFKA_BROKERS
          value: "kafka:9092"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

Deploy:

```bash
kubectl apply -f k8s/worker.yaml
```

## Scaling

### Horizontal Scaling

#### Application Servers

```bash
# Docker Compose
docker-compose up -d --scale app=5

# Kubernetes
kubectl scale deployment timeline-feed --replicas=5
```

#### Fanout Workers

```bash
# Docker Compose
docker-compose up -d --scale fanout-worker=10

# Kubernetes
kubectl scale deployment fanout-worker --replicas=10
```

### Vertical Scaling

Adjust resource limits in docker-compose.yml or k8s manifests:

```yaml
resources:
  limits:
    memory: "1Gi"
    cpu: "1000m"
```

## Monitoring

### Prometheus + Grafana

```bash
# Add monitoring stack
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts

helm install prometheus prometheus-community/kube-prometheus-stack
```

### Key Metrics

- API response time (P50, P95, P99)
- Kafka consumer lag
- Redis hit rate
- MongoDB query performance
- Error rate
- Request rate (QPS)

### Logging

```bash
# View logs
docker-compose logs -f app
kubectl logs -f deployment/timeline-feed

# Use ELK Stack for production
helm install elasticsearch elastic/elasticsearch
helm install kibana elastic/kibana
helm install filebeat elastic/filebeat
```

## Backup & Recovery

### MongoDB Backup

```bash
# Dump database
docker-compose exec mongodb mongodump \
  --db timeline_feed \
  --out /backup

# Restore
docker-compose exec mongodb mongorestore \
  --db timeline_feed \
  /backup/timeline_feed
```

### Redis Backup

```bash
# Save snapshot
docker-compose exec redis redis-cli BGSAVE

# Copy RDB file
docker cp timeline-redis:/data/dump.rdb ./backup/
```

## Performance Tuning

### MongoDB

```javascript
// Create indexes (already in mongo-init.js)
db.posts.createIndex({ userId: 1, createdAt: -1 })
db.follows.createIndex({ followerId: 1, followingId: 1 })

// Enable profiling
db.setProfilingLevel(1, { slowms: 100 })
```

### Redis

```bash
# Adjust maxmemory policy
redis-cli CONFIG SET maxmemory-policy allkeys-lru

# Monitor performance
redis-cli --latency
redis-cli INFO stats
```

### Kafka

```bash
# Increase partitions for parallelism
kafka-topics --alter --topic new-post \
  --partitions 10 \
  --bootstrap-server localhost:9092

# Adjust consumer settings
KAFKA_CONSUMER_MAX_POLL_RECORDS=500
```

## Security

### SSL/TLS

```bash
# Generate certificates
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /etc/ssl/private/nginx-selfsigned.key \
  -out /etc/ssl/certs/nginx-selfsigned.crt

# Configure Nginx
server {
  listen 443 ssl;
  ssl_certificate /etc/ssl/certs/nginx-selfsigned.crt;
  ssl_certificate_key /etc/ssl/private/nginx-selfsigned.key;
}
```

### Environment Variables

Use secrets management:

```bash
# Kubernetes Secrets
kubectl create secret generic timeline-secrets \
  --from-literal=jwt-secret=your-secret-key \
  --from-literal=mongodb-uri=mongodb://...

# Docker Secrets
echo "your-secret" | docker secret create jwt_secret -
```

## Troubleshooting

### Kafka Connection Issues

```bash
# Check Kafka status
docker-compose logs kafka

# Test connection
kafka-console-producer --broker-list localhost:9092 --topic test
```

### Redis Memory Issues

```bash
# Check memory usage
redis-cli INFO memory

# Flush cache if needed
redis-cli FLUSHALL
```

### MongoDB Slow Queries

```bash
# Enable profiling
db.setProfilingLevel(1, { slowms: 100 })

# View slow queries
db.system.profile.find().limit(10).sort({ ts: -1 })
```

## Maintenance

### Database Migration

```bash
# Run migrations
npm run migrate

# Rollback (if needed)
npm run migrate:rollback
```

### Update Dependencies

```bash
# Check for updates
npm outdated

# Update packages
npm update

# Rebuild
npm run build
```

## Cost Optimization

1. **Use reserved instances** for stable workloads
2. **Auto-scaling** for variable traffic
3. **Cache frequently accessed data** in Redis
4. **Archive old posts** to cold storage
5. **Optimize image storage** with CDN + compression

## Support

For issues or questions:
- GitHub Issues: https://github.com/your-repo/issues
- Documentation: /docs
- Email: support@example.com
