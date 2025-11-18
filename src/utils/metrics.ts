import { Request, Response, NextFunction } from 'express';
import client from 'prom-client';
import { logger } from './logger';

/**
 * Prometheus Metrics for Timeline Feed System
 * Provides comprehensive monitoring across all layers
 */
export class MetricsCollector {
  private static instance: MetricsCollector;
  public register: client.Registry;

  // HTTP Metrics
  public httpRequestsTotal: client.Counter;
  public httpRequestDuration: client.Histogram;
  public httpRequestSize: client.Histogram;
  public httpResponseSize: client.Histogram;

  // Redis Metrics
  public redisOperationsTotal: client.Counter;
  public redisOperationDuration: client.Histogram;
  public redisConnectionsActive: client.Gauge;

  // Cache Metrics
  public cacheHitsTotal: client.Counter;
  public cacheMissesTotal: client.Counter;
  public cacheHitRate: client.Gauge;

  // Circuit Breaker Metrics
  public circuitBreakerState: client.Gauge;
  public circuitBreakerFailures: client.Counter;
  public circuitBreakerSuccesses: client.Counter;

  // Kafka Metrics
  public kafkaPublishesTotal: client.Counter;
  public kafkaPublishDuration: client.Histogram;
  public kafkaConsumesTotal: client.Counter;

  // Business Metrics
  public postsCreatedTotal: client.Counter;
  public followsTotal: client.Counter;
  public timelineRequestsTotal: client.Counter;
  public activeUsers: client.Gauge;

  // System Metrics
  public systemMemoryUsage: client.Gauge;
  public systemCpuUsage: client.Gauge;

  private constructor() {
    this.register = new client.Registry();

    // Enable default metrics (CPU, memory, event loop, etc.)
    client.collectDefaultMetrics({
      register: this.register,
      prefix: 'timeline_feed_',
    });

    // Initialize HTTP Metrics
    this.httpRequestsTotal = new client.Counter({
      name: 'http_requests_total',
      help: 'Total number of HTTP requests',
      labelNames: ['method', 'route', 'status'],
      registers: [this.register],
    });

    this.httpRequestDuration = new client.Histogram({
      name: 'http_request_duration_ms',
      help: 'Duration of HTTP requests in milliseconds',
      labelNames: ['method', 'route', 'status'],
      buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
      registers: [this.register],
    });

    this.httpRequestSize = new client.Histogram({
      name: 'http_request_size_bytes',
      help: 'Size of HTTP requests in bytes',
      labelNames: ['method', 'route'],
      buckets: [100, 1000, 10000, 100000, 1000000],
      registers: [this.register],
    });

    this.httpResponseSize = new client.Histogram({
      name: 'http_response_size_bytes',
      help: 'Size of HTTP responses in bytes',
      labelNames: ['method', 'route'],
      buckets: [100, 1000, 10000, 100000, 1000000],
      registers: [this.register],
    });

    // Initialize Redis Metrics
    this.redisOperationsTotal = new client.Counter({
      name: 'redis_operations_total',
      help: 'Total number of Redis operations',
      labelNames: ['operation', 'status'],
      registers: [this.register],
    });

    this.redisOperationDuration = new client.Histogram({
      name: 'redis_operation_duration_ms',
      help: 'Duration of Redis operations in milliseconds',
      labelNames: ['operation'],
      buckets: [1, 5, 10, 25, 50, 100, 250, 500],
      registers: [this.register],
    });

    this.redisConnectionsActive = new client.Gauge({
      name: 'redis_connections_active',
      help: 'Number of active Redis connections',
      registers: [this.register],
    });

    // Initialize Cache Metrics
    this.cacheHitsTotal = new client.Counter({
      name: 'cache_hits_total',
      help: 'Total number of cache hits',
      labelNames: ['cache_type'],
      registers: [this.register],
    });

    this.cacheMissesTotal = new client.Counter({
      name: 'cache_misses_total',
      help: 'Total number of cache misses',
      labelNames: ['cache_type'],
      registers: [this.register],
    });

    this.cacheHitRate = new client.Gauge({
      name: 'cache_hit_rate',
      help: 'Cache hit rate (0-1)',
      labelNames: ['cache_type'],
      registers: [this.register],
    });

    // Initialize Circuit Breaker Metrics
    this.circuitBreakerState = new client.Gauge({
      name: 'circuit_breaker_state',
      help: 'Circuit breaker state (0=closed, 1=half-open, 2=open)',
      labelNames: ['breaker_name'],
      registers: [this.register],
    });

    this.circuitBreakerFailures = new client.Counter({
      name: 'circuit_breaker_failures_total',
      help: 'Total number of circuit breaker failures',
      labelNames: ['breaker_name'],
      registers: [this.register],
    });

    this.circuitBreakerSuccesses = new client.Counter({
      name: 'circuit_breaker_successes_total',
      help: 'Total number of circuit breaker successes',
      labelNames: ['breaker_name'],
      registers: [this.register],
    });

    // Initialize Kafka Metrics
    this.kafkaPublishesTotal = new client.Counter({
      name: 'kafka_publishes_total',
      help: 'Total number of Kafka publishes',
      labelNames: ['topic', 'status'],
      registers: [this.register],
    });

    this.kafkaPublishDuration = new client.Histogram({
      name: 'kafka_publish_duration_ms',
      help: 'Duration of Kafka publish operations in milliseconds',
      labelNames: ['topic'],
      buckets: [10, 50, 100, 250, 500, 1000, 2500, 5000],
      registers: [this.register],
    });

    this.kafkaConsumesTotal = new client.Counter({
      name: 'kafka_consumes_total',
      help: 'Total number of Kafka message consumptions',
      labelNames: ['topic', 'status'],
      registers: [this.register],
    });

    // Initialize Business Metrics
    this.postsCreatedTotal = new client.Counter({
      name: 'posts_created_total',
      help: 'Total number of posts created',
      registers: [this.register],
    });

    this.followsTotal = new client.Counter({
      name: 'follows_total',
      help: 'Total number of follow/unfollow actions',
      labelNames: ['action'],
      registers: [this.register],
    });

    this.timelineRequestsTotal = new client.Counter({
      name: 'timeline_requests_total',
      help: 'Total number of timeline requests',
      labelNames: ['type'],
      registers: [this.register],
    });

    this.activeUsers = new client.Gauge({
      name: 'active_users',
      help: 'Number of currently active users',
      registers: [this.register],
    });

    // Initialize System Metrics
    this.systemMemoryUsage = new client.Gauge({
      name: 'system_memory_usage_bytes',
      help: 'System memory usage in bytes',
      labelNames: ['type'],
      registers: [this.register],
    });

    this.systemCpuUsage = new client.Gauge({
      name: 'system_cpu_usage_percent',
      help: 'System CPU usage percentage',
      registers: [this.register],
    });

    // Update system metrics every 10 seconds
    this.startSystemMetricsCollection();

    logger.info('Prometheus metrics initialized');
  }

  public static getInstance(): MetricsCollector {
    if (!MetricsCollector.instance) {
      MetricsCollector.instance = new MetricsCollector();
    }
    return MetricsCollector.instance;
  }

  /**
   * Middleware to collect HTTP metrics
   */
  public httpMetricsMiddleware() {
    return (req: Request, res: Response, next: NextFunction): void => {
      const start = Date.now();

      // Request size
      const requestSize = parseInt(req.get('content-length') || '0', 10);
      if (requestSize > 0) {
        this.httpRequestSize.observe(
          { method: req.method, route: this.normalizeRoute(req.path) },
          requestSize
        );
      }

      // Response finished listener
      res.on('finish', () => {
        const duration = Date.now() - start;
        const route = this.normalizeRoute(req.path);
        const status = res.statusCode.toString();

        // Record metrics
        this.httpRequestsTotal.inc({ method: req.method, route, status });
        this.httpRequestDuration.observe({ method: req.method, route, status }, duration);

        // Response size
        const responseSize = parseInt(res.get('content-length') || '0', 10);
        if (responseSize > 0) {
          this.httpResponseSize.observe({ method: req.method, route }, responseSize);
        }

        // Log slow requests
        if (duration > 1000) {
          logger.warn(`Slow request: ${req.method} ${req.path} took ${duration}ms`);
        }
      });

      next();
    };
  }

  /**
   * Record Redis operation
   */
  public recordRedisOperation(operation: string, duration: number, success: boolean): void {
    this.redisOperationsTotal.inc({
      operation,
      status: success ? 'success' : 'failure',
    });
    this.redisOperationDuration.observe({ operation }, duration);
  }

  /**
   * Record cache hit/miss
   */
  public recordCacheAccess(cacheType: string, hit: boolean): void {
    if (hit) {
      this.cacheHitsTotal.inc({ cache_type: cacheType });
    } else {
      this.cacheMissesTotal.inc({ cache_type: cacheType });
    }

    // Update hit rate
    this.updateCacheHitRate(cacheType);
  }

  /**
   * Update circuit breaker state
   */
  public updateCircuitBreakerState(
    name: string,
    state: 'closed' | 'half-open' | 'open'
  ): void {
    const stateValue = state === 'closed' ? 0 : state === 'half-open' ? 1 : 2;
    this.circuitBreakerState.set({ breaker_name: name }, stateValue);
  }

  /**
   * Record circuit breaker operation
   */
  public recordCircuitBreakerOperation(name: string, success: boolean): void {
    if (success) {
      this.circuitBreakerSuccesses.inc({ breaker_name: name });
    } else {
      this.circuitBreakerFailures.inc({ breaker_name: name });
    }
  }

  /**
   * Record Kafka publish
   */
  public recordKafkaPublish(topic: string, duration: number, success: boolean): void {
    this.kafkaPublishesTotal.inc({
      topic,
      status: success ? 'success' : 'failure',
    });
    this.kafkaPublishDuration.observe({ topic }, duration);
  }

  /**
   * Record Kafka consume
   */
  public recordKafkaConsume(topic: string, success: boolean): void {
    this.kafkaConsumesTotal.inc({
      topic,
      status: success ? 'success' : 'failure',
    });
  }

  /**
   * Get metrics in Prometheus format
   */
  public async getMetrics(): Promise<string> {
    return await this.register.metrics();
  }

  /**
   * Get metrics as JSON
   */
  public async getMetricsJSON(): Promise<any> {
    const metrics = await this.register.getMetricsAsJSON();
    return metrics;
  }

  /**
   * Normalize route for consistent metric labels
   */
  private normalizeRoute(path: string): string {
    // Replace UUIDs with :id
    const normalized = path.replace(
      /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi,
      ':id'
    );

    // Replace numeric IDs
    return normalized.replace(/\/\d+/g, '/:id');
  }

  /**
   * Calculate and update cache hit rate
   */
  private updateCacheHitRate(cacheType: string): void {
    const hits = (this.cacheHitsTotal as any).hashMap[`cache_type:${cacheType}`]?.value || 0;
    const misses = (this.cacheMissesTotal as any).hashMap[`cache_type:${cacheType}`]?.value || 0;
    const total = hits + misses;

    if (total > 0) {
      const hitRate = hits / total;
      this.cacheHitRate.set({ cache_type: cacheType }, hitRate);
    }
  }

  /**
   * Start collecting system metrics
   */
  private startSystemMetricsCollection(): void {
    setInterval(() => {
      const memUsage = process.memoryUsage();

      this.systemMemoryUsage.set({ type: 'rss' }, memUsage.rss);
      this.systemMemoryUsage.set({ type: 'heapTotal' }, memUsage.heapTotal);
      this.systemMemoryUsage.set({ type: 'heapUsed' }, memUsage.heapUsed);
      this.systemMemoryUsage.set({ type: 'external' }, memUsage.external);

      // CPU usage (approximation)
      const cpuUsage = process.cpuUsage();
      const totalUsage = (cpuUsage.user + cpuUsage.system) / 1000000; // Convert to seconds
      this.systemCpuUsage.set(totalUsage);
    }, 10000); // Every 10 seconds
  }
}

// Export singleton instance
export const metricsCollector = MetricsCollector.getInstance();
