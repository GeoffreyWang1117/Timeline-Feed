import { Request, Response, NextFunction } from 'express';
import {
  validateUUID,
  validateContent,
  validateMediaUrls,
  validateCursor,
  validatePagination,
} from '../validation';
import { AppError } from '../errorHandler';

describe('Validation Middleware', () => {
  let mockReq: Partial<Request>;
  let mockRes: Partial<Response>;
  let mockNext: NextFunction;

  beforeEach(() => {
    mockReq = {
      params: {},
      query: {},
      body: {},
    };
    mockRes = {};
    mockNext = jest.fn();
  });

  describe('validateUUID', () => {
    it('should pass for valid UUID v4', () => {
      const validUUID = '550e8400-e29b-41d4-a716-446655440000';
      mockReq.params = { userId: validUUID };

      const middleware = validateUUID('userId');
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalled();
      expect(mockNext).toHaveBeenCalledWith(); // Called without error
    });

    it('should throw AppError for invalid UUID format', () => {
      mockReq.params = { userId: 'not-a-uuid' };

      const middleware = validateUUID('userId');

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow(AppError);

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Invalid userId: must be a valid UUID v4');
    });

    it('should throw AppError for missing parameter', () => {
      mockReq.params = {};

      const middleware = validateUUID('userId');

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow(AppError);

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Missing parameter: userId');
    });

    it('should reject UUID v1/v3/v5 (only v4 allowed)', () => {
      // UUID v1
      const uuidV1 = '6ba7b810-9dad-11d1-80b4-00c04fd430c8';
      mockReq.params = { postId: uuidV1 };

      const middleware = validateUUID('postId');

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Invalid postId: must be a valid UUID v4');
    });

    it('should reject malicious SQL injection attempts', () => {
      mockReq.params = { userId: "'; DROP TABLE users; --" };

      const middleware = validateUUID('userId');

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow(AppError);
    });
  });

  describe('validateContent', () => {
    it('should pass for valid content', () => {
      mockReq.body = { content: 'This is a valid post content!' };

      const middleware = validateContent();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalled();
      expect(mockReq.body.content).toBe('This is a valid post content!');
    });

    it('should escape HTML to prevent XSS', () => {
      mockReq.body = { content: '<script>alert("XSS")</script>' };

      const middleware = validateContent();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      // Content should be escaped
      expect(mockReq.body.content).toContain('&lt;script&gt;');
      expect(mockReq.body.content).not.toContain('<script>');
      expect(mockNext).toHaveBeenCalled();
    });

    it('should trim whitespace', () => {
      mockReq.body = { content: '  Trimmed content  ' };

      const middleware = validateContent();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockReq.body.content).toBe('Trimmed content');
      expect(mockNext).toHaveBeenCalled();
    });

    it('should throw error for content exceeding 280 characters', () => {
      const longContent = 'a'.repeat(281);
      mockReq.body = { content: longContent };

      const middleware = validateContent();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow(AppError);

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Content exceeds maximum length of 280 characters');
    });

    it('should throw error for empty content after trimming', () => {
      mockReq.body = { content: '   ' };

      const middleware = validateContent();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Content cannot be empty');
    });

    it('should throw error for missing content', () => {
      mockReq.body = {};

      const middleware = validateContent();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Content is required');
    });

    it('should handle complex XSS attempts', () => {
      const xssAttempts = [
        '<img src=x onerror=alert(1)>',
        '<svg onload=alert(1)>',
        'javascript:alert(1)',
        '<iframe src="javascript:alert(1)"></iframe>',
      ];

      xssAttempts.forEach((xss) => {
        mockReq.body = { content: xss };
        mockNext = jest.fn();

        const middleware = validateContent();
        middleware(mockReq as Request, mockRes as Response, mockNext);

        // Should escape all HTML
        expect(mockReq.body.content).not.toContain('<');
        expect(mockReq.body.content).not.toContain('>');
        expect(mockNext).toHaveBeenCalled();
      });
    });
  });

  describe('validateMediaUrls', () => {
    it('should pass for valid HTTPS URLs', () => {
      mockReq.body = {
        mediaUrls: [
          'https://i.imgur.com/abc123.jpg',
          'https://imgur.com/def456.png',
        ],
      };

      const middleware = validateMediaUrls();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalled();
    });

    it('should pass when mediaUrls is undefined', () => {
      mockReq.body = {};

      const middleware = validateMediaUrls();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalled();
    });

    it('should reject HTTP URLs (require HTTPS)', () => {
      mockReq.body = {
        mediaUrls: ['http://imgur.com/image.jpg'],
      };

      const middleware = validateMediaUrls();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('mediaUrls[0] is not a valid HTTPS URL');
    });

    it('should reject more than 4 media URLs', () => {
      mockReq.body = {
        mediaUrls: [
          'https://imgur.com/1.jpg',
          'https://imgur.com/2.jpg',
          'https://imgur.com/3.jpg',
          'https://imgur.com/4.jpg',
          'https://imgur.com/5.jpg', // 5th URL
        ],
      };

      const middleware = validateMediaUrls();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Maximum 4 media URLs allowed');
    });

    it('should reject invalid URL format', () => {
      mockReq.body = {
        mediaUrls: ['not-a-url'],
      };

      const middleware = validateMediaUrls();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('mediaUrls[0] is not a valid HTTPS URL');
    });

    it('should throw error when mediaUrls is not an array', () => {
      mockReq.body = {
        mediaUrls: 'https://imgur.com/image.jpg', // String instead of array
      };

      const middleware = validateMediaUrls();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('mediaUrls must be an array');
    });
  });

  describe('validateCursor', () => {
    it('should pass for valid base64 cursor', () => {
      const validCursor = Buffer.from('{"timestamp":1234567890,"postId":"abc"}').toString(
        'base64'
      );
      mockReq.query = { cursor: validCursor };

      const middleware = validateCursor();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalled();
    });

    it('should pass when cursor is not provided', () => {
      mockReq.query = {};

      const middleware = validateCursor();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalled();
    });

    it('should throw error for invalid base64 format', () => {
      mockReq.query = { cursor: 'not-valid-base64!!!' };

      const middleware = validateCursor();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Invalid cursor format');
    });

    it('should reject cursor exceeding length limit (DoS prevention)', () => {
      const longCursor = 'A'.repeat(1001);
      mockReq.query = { cursor: longCursor };

      const middleware = validateCursor();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Cursor too long');
    });

    it('should throw error if cursor is not a string', () => {
      mockReq.query = { cursor: 123 as any };

      const middleware = validateCursor();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('Cursor must be a string');
    });
  });

  describe('validatePagination', () => {
    it('should pass for valid pagination parameters', () => {
      mockReq.query = { limit: '50', offset: '10' };

      const middleware = validatePagination();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalled();
    });

    it('should pass when pagination parameters are omitted', () => {
      mockReq.query = {};

      const middleware = validatePagination();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalled();
    });

    it('should reject limit exceeding maximum (100)', () => {
      mockReq.query = { limit: '150' };

      const middleware = validatePagination();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('limit must be at most 100');
    });

    it('should reject limit less than 1', () => {
      mockReq.query = { limit: '0' };

      const middleware = validatePagination();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('limit must be at least 1');
    });

    it('should reject negative offset', () => {
      mockReq.query = { offset: '-5' };

      const middleware = validatePagination();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('offset must be at least 0');
    });

    it('should reject non-numeric limit', () => {
      mockReq.query = { limit: 'abc' };

      const middleware = validatePagination();

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow('limit must be a number');
    });
  });

  describe('Integration - Multiple Validations', () => {
    it('should validate complete post creation request', () => {
      const validUUID = '550e8400-e29b-41d4-a716-446655440000';
      mockReq.params = { userId: validUUID };
      mockReq.body = {
        content: 'Valid post with #hashtag and @mention',
        mediaUrls: ['https://imgur.com/image.jpg'],
      };

      // Apply all validations
      validateUUID('userId')(mockReq as Request, mockRes as Response, mockNext);
      validateContent()(mockReq as Request, mockRes as Response, mockNext);
      validateMediaUrls()(mockReq as Request, mockRes as Response, mockNext);

      expect(mockNext).toHaveBeenCalledTimes(3);
      // Content should be escaped
      expect(mockReq.body.content).toBe('Valid post with #hashtag and @mention');
    });
  });

  describe('Security - Attack Prevention', () => {
    it('should prevent NoSQL injection in UUID parameter', () => {
      mockReq.params = { userId: { $gt: '' } as any };

      const middleware = validateUUID('userId');

      expect(() => {
        middleware(mockReq as Request, mockRes as Response, mockNext);
      }).toThrow(AppError);
    });

    it('should prevent XSS in content with multiple vectors', () => {
      const xssPayload = `
        <script>alert('XSS')</script>
        <img src=x onerror=alert(1)>
        <svg/onload=alert(1)>
      `;
      mockReq.body = { content: xssPayload };

      const middleware = validateContent();
      middleware(mockReq as Request, mockRes as Response, mockNext);

      // All script tags should be escaped
      expect(mockReq.body.content).not.toMatch(/<script>/i);
      expect(mockReq.body.content).not.toMatch(/<img/i);
      expect(mockReq.body.content).not.toMatch(/<svg/i);
      expect(mockReq.body.content).toContain('&lt;');
    });
  });
});
