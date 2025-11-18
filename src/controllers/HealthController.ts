import { Request, Response } from 'express';
import mongoose from 'mongoose';
import { redis } from '../config/database';
import { logger } from '../utils/logger';
import { kafkaService } from '../services/KafkaService';

/**
 * Health Check Controller
 * Provides liveness and readiness probes for Kubernetes/Docker
 */
export class HealthController {
  /**
   * Basic liveness probe
   * Returns 200 if the application is running
   */
  async liveness(req: Request, res: Response): Promise<void> {
    res.status(200).json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      environment: process.env.NODE_ENV || 'development',
    });
  }

  /**
   * Readiness probe with deep health checks
   * Returns 200 only if all dependencies are healthy
   */
  async readiness(req: Request, res: Response): Promise<void> {
    const checks = {
      mongodb: await this.checkMongoDB(),
      redis: await this.checkRedis(),
      kafka: await this.checkKafka(),
    };

    const allHealthy = Object.values(checks).every((check) => check.healthy);
    const status = allHealthy ? 'ready' : 'not_ready';
    const statusCode = allHealthy ? 200 : 503;

    res.status(statusCode).json({
      status,
      timestamp: new Date().toISOString(),
      checks,
    });
  }

  /**
   * Detailed health status
   * Provides comprehensive information about system health
   */
  async detailed(req: Request, res: Response): Promise<void> {
    const [mongodb, redis, kafka, system] = await Promise.all([
      this.checkMongoDB(),
      this.checkRedis(),
      this.checkKafka(),
      this.getSystemInfo(),
    ]);

    const allHealthy = [mongodb, redis, kafka].every((check) => check.healthy);

    res.status(allHealthy ? 200 : 503).json({
      status: allHealthy ? 'healthy' : 'degraded',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      environment: process.env.NODE_ENV || 'development',
      checks: {
        mongodb,
        redis,
        kafka,
      },
      system,
    });
  }

  /**
   * Check MongoDB connection
   */
  private async checkMongoDB(): Promise<HealthCheckResult> {
    const start = Date.now();
    try {
      if (mongoose.connection.readyState === 1) {
        // Perform a simple query to verify connection
        await mongoose.connection.db.admin().ping();

        return {
          healthy: true,
          responseTime: Date.now() - start,
          details: {
            state: 'connected',
            database: mongoose.connection.db.databaseName,
          },
        };
      } else {
        return {
          healthy: false,
          responseTime: Date.now() - start,
          details: {
            state: 'disconnected',
            error: 'MongoDB not connected',
          },
        };
      }
    } catch (error: any) {
      logger.error('MongoDB health check failed:', error);
      return {
        healthy: false,
        responseTime: Date.now() - start,
        details: {
          state: 'error',
          error: error.message,
        },
      };
    }
  }

  /**
   * Check Redis connection
   */
  private async checkRedis(): Promise<HealthCheckResult> {
    const start = Date.now();
    try {
      const pong = await redis.ping();

      if (pong === 'PONG') {
        const info = await redis.info('stats');
        const connections = this.parseRedisInfo(info, 'total_connections_received');

        return {
          healthy: true,
          responseTime: Date.now() - start,
          details: {
            state: 'connected',
            totalConnections: connections,
          },
        };
      } else {
        return {
          healthy: false,
          responseTime: Date.now() - start,
          details: {
            state: 'error',
            error: 'Unexpected ping response',
          },
        };
      }
    } catch (error: any) {
      logger.error('Redis health check failed:', error);
      return {
        healthy: false,
        responseTime: Date.now() - start,
        details: {
          state: 'error',
          error: error.message,
        },
      };
    }
  }

  /**
   * Check Kafka connection
   */
  private async checkKafka(): Promise<HealthCheckResult> {
    const start = Date.now();
    try {
      // Simple check - verify producer is initialized
      // In production, you might want to do a test publish
      const isHealthy = (kafkaService as any).producer !== null;

      return {
        healthy: isHealthy,
        responseTime: Date.now() - start,
        details: {
          state: isHealthy ? 'connected' : 'disconnected',
          producer: isHealthy ? 'initialized' : 'not_initialized',
        },
      };
    } catch (error: any) {
      logger.error('Kafka health check failed:', error);
      return {
        healthy: false,
        responseTime: Date.now() - start,
        details: {
          state: 'error',
          error: error.message,
        },
      };
    }
  }

  /**
   * Get system information
   */
  private getSystemInfo(): SystemInfo {
    const memUsage = process.memoryUsage();
    const cpuUsage = process.cpuUsage();

    return {
      memory: {
        rss: this.formatBytes(memUsage.rss),
        heapTotal: this.formatBytes(memUsage.heapTotal),
        heapUsed: this.formatBytes(memUsage.heapUsed),
        external: this.formatBytes(memUsage.external),
      },
      cpu: {
        user: cpuUsage.user / 1000000, // Convert to seconds
        system: cpuUsage.system / 1000000,
      },
      uptime: process.uptime(),
      nodeVersion: process.version,
      platform: process.platform,
      arch: process.arch,
    };
  }

  /**
   * Format bytes to human-readable format
   */
  private formatBytes(bytes: number): string {
    const mb = bytes / 1024 / 1024;
    return `${mb.toFixed(2)} MB`;
  }

  /**
   * Parse Redis INFO output
   */
  private parseRedisInfo(info: string, key: string): string {
    const regex = new RegExp(`${key}:(\\d+)`);
    const match = info.match(regex);
    return match ? match[1] : 'unknown';
  }
}

// Types
interface HealthCheckResult {
  healthy: boolean;
  responseTime: number;
  details: any;
}

interface SystemInfo {
  memory: {
    rss: string;
    heapTotal: string;
    heapUsed: string;
    external: string;
  };
  cpu: {
    user: number;
    system: number;
  };
  uptime: number;
  nodeVersion: string;
  platform: string;
  arch: string;
}

export const healthController = new HealthController();
