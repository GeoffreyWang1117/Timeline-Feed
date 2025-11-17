import { Request, Response, NextFunction } from 'express';
import { AppError } from './errorHandler';
import { validate as uuidValidate, version as uuidVersion } from 'uuid';
import validator from 'validator';

/**
 * Validate UUID parameter
 */
export const validateUUID = (paramName: string) => {
  return (req: Request, res: Response, next: NextFunction): void => {
    const value = req.params[paramName];

    if (!value) {
      throw new AppError(`Missing parameter: ${paramName}`, 400, 'MISSING_PARAMETER');
    }

    if (!uuidValidate(value) || uuidVersion(value) !== 4) {
      throw new AppError(
        `Invalid ${paramName}: must be a valid UUID v4`,
        400,
        'INVALID_ID'
      );
    }

    next();
  };
};

/**
 * Validate query parameters
 */
export const validateQueryParams = (schema: {
  [key: string]: {
    type: 'number' | 'string' | 'boolean';
    min?: number;
    max?: number;
    pattern?: RegExp;
    optional?: boolean;
  };
}) => {
  return (req: Request, res: Response, next: NextFunction): void => {
    const errors: string[] = [];

    Object.entries(schema).forEach(([key, rules]) => {
      const value = req.query[key];

      // Check if required
      if (!rules.optional && !value) {
        errors.push(`Missing required query parameter: ${key}`);
        return;
      }

      // Skip validation if optional and not provided
      if (rules.optional && !value) {
        return;
      }

      // Type validation
      switch (rules.type) {
        case 'number':
          const num = Number(value);
          if (isNaN(num)) {
            errors.push(`${key} must be a number`);
          } else {
            // Range validation
            if (rules.min !== undefined && num < rules.min) {
              errors.push(`${key} must be at least ${rules.min}`);
            }
            if (rules.max !== undefined && num > rules.max) {
              errors.push(`${key} must be at most ${rules.max}`);
            }
          }
          break;

        case 'string':
          if (typeof value !== 'string') {
            errors.push(`${key} must be a string`);
          } else {
            // Pattern validation
            if (rules.pattern && !rules.pattern.test(value)) {
              errors.push(`${key} has invalid format`);
            }
          }
          break;

        case 'boolean':
          if (value !== 'true' && value !== 'false') {
            errors.push(`${key} must be true or false`);
          }
          break;
      }
    });

    if (errors.length > 0) {
      throw new AppError(errors.join(', '), 400, 'INVALID_QUERY_PARAMS');
    }

    next();
  };
};

/**
 * Sanitize user input to prevent XSS
 */
export const sanitizeInput = (fields: string[]) => {
  return (req: Request, res: Response, next: NextFunction): void => {
    fields.forEach((field) => {
      if (req.body[field] && typeof req.body[field] === 'string') {
        // Escape HTML
        req.body[field] = validator.escape(req.body[field]);
      }
    });

    next();
  };
};

/**
 * Validate email format
 */
export const validateEmail = (fieldName: string = 'email') => {
  return (req: Request, res: Response, next: NextFunction): void => {
    const email = req.body[fieldName];

    if (!email) {
      throw new AppError(`${fieldName} is required`, 400, 'MISSING_FIELD');
    }

    if (!validator.isEmail(email)) {
      throw new AppError(`Invalid ${fieldName} format`, 400, 'INVALID_EMAIL');
    }

    // Normalize email
    req.body[fieldName] = validator.normalizeEmail(email);

    next();
  };
};

/**
 * Validate and sanitize content (for posts)
 */
export const validateContent = () => {
  return (req: Request, res: Response, next: NextFunction): void => {
    let { content } = req.body;

    if (!content) {
      throw new AppError('Content is required', 400, 'MISSING_CONTENT');
    }

    if (typeof content !== 'string') {
      throw new AppError('Content must be a string', 400, 'INVALID_CONTENT_TYPE');
    }

    // Trim whitespace
    content = content.trim();

    // Check length
    if (content.length === 0) {
      throw new AppError('Content cannot be empty', 400, 'EMPTY_CONTENT');
    }

    if (content.length > 280) {
      throw new AppError(
        'Content exceeds maximum length of 280 characters',
        400,
        'CONTENT_TOO_LONG'
      );
    }

    // Escape HTML to prevent XSS
    content = validator.escape(content);

    // Update request body
    req.body.content = content;

    next();
  };
};

/**
 * Validate media URLs
 */
export const validateMediaUrls = () => {
  return (req: Request, res: Response, next: NextFunction): void => {
    const { mediaUrls } = req.body;

    if (!mediaUrls) {
      return next();
    }

    if (!Array.isArray(mediaUrls)) {
      throw new AppError('mediaUrls must be an array', 400, 'INVALID_MEDIA_URLS');
    }

    if (mediaUrls.length > 4) {
      throw new AppError('Maximum 4 media URLs allowed', 400, 'TOO_MANY_MEDIA');
    }

    // Validate each URL
    const allowedDomains = ['imgur.com', 'i.imgur.com', 'cloudinary.com'];
    const errors: string[] = [];

    mediaUrls.forEach((url, index) => {
      // Check if valid URL
      if (!validator.isURL(url, { protocols: ['https'], require_protocol: true })) {
        errors.push(`mediaUrls[${index}] is not a valid HTTPS URL`);
        return;
      }

      // Check if allowed domain (optional security measure)
      const urlObj = new URL(url);
      const isAllowedDomain = allowedDomains.some((domain) =>
        urlObj.hostname.endsWith(domain)
      );

      if (!isAllowedDomain && process.env.NODE_ENV === 'production') {
        errors.push(
          `mediaUrls[${index}] domain not allowed. Allowed: ${allowedDomains.join(', ')}`
        );
      }
    });

    if (errors.length > 0) {
      throw new AppError(errors.join(', '), 400, 'INVALID_MEDIA_URLS');
    }

    next();
  };
};

/**
 * Validate cursor token
 */
export const validateCursor = () => {
  return (req: Request, res: Response, next: NextFunction): void => {
    const { cursor } = req.query;

    if (!cursor) {
      return next();
    }

    if (typeof cursor !== 'string') {
      throw new AppError('Cursor must be a string', 400, 'INVALID_CURSOR');
    }

    // Check if valid base64
    if (!validator.isBase64(cursor)) {
      throw new AppError('Invalid cursor format', 400, 'INVALID_CURSOR');
    }

    // Check reasonable length (prevents DoS)
    if (cursor.length > 1000) {
      throw new AppError('Cursor too long', 400, 'CURSOR_TOO_LONG');
    }

    next();
  };
};

/**
 * Validate pagination parameters
 */
export const validatePagination = () => {
  return validateQueryParams({
    limit: {
      type: 'number',
      min: 1,
      max: 100,
      optional: true,
    },
    offset: {
      type: 'number',
      min: 0,
      optional: true,
    },
  });
};
