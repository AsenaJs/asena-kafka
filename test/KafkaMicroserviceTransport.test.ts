import { describe, expect, it } from 'bun:test';
import { KafkaMicroserviceTransport, TopicNaming, assertTopicLegal } from '../lib/microservice';
import {
  buildContext,
  buildEventHeaders,
  buildRequestHeaders,
  decodeHeaders,
  decodeMarker,
  encodeMarker,
  parsePayload,
} from '../lib/microservice/envelope';

const CONFIG = { brokers: ['localhost:9092'] };

function makeTransport(options: Record<string, unknown> = {}): KafkaMicroserviceTransport {
  return new KafkaMicroserviceTransport(CONFIG, { serviceName: 'test-service', ...options } as any);
}

// Offline unit tests - no broker involved.

describe('KafkaMicroserviceTransport constructor', () => {
  it('should throw without serviceName', () => {
    expect(() => new KafkaMicroserviceTransport(CONFIG, {} as any)).toThrow('requires a serviceName');
  });

  it('should throw when serviceName is not topic-legal', () => {
    expect(() => makeTransport({ serviceName: 'not:legal' })).toThrow('Kafka-topic-legal');
  });

  it('should throw when topicPrefix is not topic-legal', () => {
    expect(() => makeTransport({ topicPrefix: 'asena:ms' })).toThrow('Kafka-topic-legal');
  });

  it('should throw when handlerTimeout exceeds sessionTimeout', () => {
    expect(() => makeTransport({ sessionTimeout: 5000, handlerTimeout: 6000 })).toThrow(
      'must not exceed sessionTimeout',
    );
  });

  it('should derive handlerTimeout from a lowered sessionTimeout', () => {
    // No throw: default handlerTimeout must follow sessionTimeout down
    expect(() => makeTransport({ sessionTimeout: 3000 })).not.toThrow();
  });

  it('should not be connected before init', () => {
    expect(makeTransport().isConnected).toBe(false);
  });
});

describe('handler registration validation', () => {
  it('should reject an empty message pattern', () => {
    expect(() => makeTransport().registerMessageHandler('', async () => {})).toThrow('cannot be empty');
  });

  it('should reject wildcards in message patterns', () => {
    expect(() => makeTransport().registerMessageHandler('order.*', async () => {})).toThrow('cannot contain wildcards');
  });

  it('should reject message patterns with topic-illegal characters', () => {
    expect(() => makeTransport().registerMessageHandler('order/create', async () => {})).toThrow('Kafka-topic-legal');
  });

  it('should reject duplicate message patterns', () => {
    const transport = makeTransport();

    transport.registerMessageHandler('order.create', async () => {});
    expect(() => transport.registerMessageHandler('order.create', async () => {})).toThrow('Duplicate');
  });

  it('should reject an empty event pattern', () => {
    expect(() => makeTransport().registerEventHandler('', async () => {})).toThrow('cannot be empty');
  });

  it('should allow wildcards in event patterns', () => {
    // Event patterns are matched locally against the `p` header - they never
    // become topic names, so wildcards and exotic characters are fine
    expect(() => makeTransport().registerEventHandler('booking.*', async () => {})).not.toThrow();
  });
});

describe('destroy before init', () => {
  it('should be safe and idempotent', async () => {
    const transport = makeTransport();

    await transport.destroy();
    await transport.destroy();

    expect(transport.isConnected).toBe(false);
  });
});

describe('TopicNaming', () => {
  const naming = new TopicNaming('asena.ms');

  it('should build topic names', () => {
    expect(naming.eventTopic).toBe('asena.ms.evt');
    expect(naming.dlqTopic).toBe('asena.ms.dlq');
    expect(naming.replyTopic).toBe('asena.ms.reply');
    expect(naming.requestTopic('order.create')).toBe('asena.ms.req.order.create');
  });

  it('should recognize and invert request topics', () => {
    expect(naming.isRequestTopic('asena.ms.req.order.create')).toBe(true);
    expect(naming.isRequestTopic('asena.ms.evt')).toBe(false);
    expect(naming.patternOf('asena.ms.req.order.create')).toBe('order.create');
  });

  it('should scope group ids by prefix', () => {
    expect(naming.groupId('booking-service')).toBe('asena.ms.booking-service');
    expect(naming.replyGroupId('booking-service', 'abcdef12-3456')).toBe('asena.ms.booking-service.reply.abcdef12');
  });

  it('should reject illegal prefixes', () => {
    expect(() => new TopicNaming('asena:ms')).toThrow('Kafka-topic-legal');
    expect(() => assertTopicLegal('a'.repeat(250), 'name')).toThrow('249');
  });
});

describe('envelope', () => {
  it('should round-trip event headers', () => {
    const headers = buildEventHeaders('booking.created', 'mid-1', { 'x-tenant': 'acme' });

    expect(headers['p']).toBe('booking.created');
    expect(headers['mid']).toBe('mid-1');
    expect(JSON.parse(headers['h'])).toEqual({ 'x-tenant': 'acme' });
    expect(Number(headers['ts'])).toBeGreaterThan(0);
  });

  it('should extend request headers with correlation fields', () => {
    const headers = buildRequestHeaders('order.get', 'mid-2', undefined, 'corr-1', 'asena.ms.reply', 5000);

    expect(headers['c']).toBe('corr-1');
    expect(headers['r']).toBe('asena.ms.reply');
    expect(headers['to']).toBe('5000');
    expect(JSON.parse(headers['h'])).toEqual({});
  });

  it('should decode Buffer header values to strings', () => {
    expect(decodeHeaders({ p: Buffer.from('a.b'), mid: 'x', gone: undefined })).toEqual({ p: 'a.b', mid: 'x' });
  });

  it('should build a MessageContext from a payload', () => {
    const context = buildContext(
      {
        topic: 't',
        partition: 0,
        message: {
          key: null,
          value: Buffer.from('{"a":1}'),
          offset: '7',
          timestamp: '1700000000000',
          headers: buildRequestHeaders('order.get', 'mid-3', { k: 'v' }, 'corr-2', 'reply', 1000),
        },
        heartbeat: async () => {},
      },
      2,
    );

    expect(context.pattern).toBe('order.get');
    expect(context.messageId).toBe('mid-3');
    expect(context.correlationId).toBe('corr-2');
    expect(context.headers).toEqual({ k: 'v' });
    expect(context.attempt).toBe(2);
  });

  it('should fall back to a position-based messageId for foreign records', () => {
    const context = buildContext(
      {
        topic: 'ext',
        partition: 3,
        message: { key: null, value: null, offset: '42', timestamp: '0', headers: {} },
        heartbeat: async () => {},
      },
      1,
    );

    expect(context.messageId).toBe('ext:3:42');
  });

  it('should parse payloads with a raw-string fallback', () => {
    expect(parsePayload(Buffer.from('{"x":1}'))).toEqual({ x: 1 });
    expect(parsePayload(Buffer.from('not-json'))).toBe('not-json');
    expect(parsePayload(null)).toBeNull();
  });

  it('should round-trip attempt markers and reject garbage', () => {
    expect(decodeMarker(encodeMarker(3))).toBe(3);
    expect(decodeMarker(null)).toBeNull();
    expect(decodeMarker('')).toBeNull();
    expect(decodeMarker('not-json')).toBeNull();
    expect(decodeMarker('{"a":0}')).toBeNull();
    expect(decodeMarker('{"b":2}')).toBeNull();
  });
});
