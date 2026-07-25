import type { MessageContext } from '@asenajs/asena/microservice';
import type { KafkaEachMessagePayload, KafkaHeaderValue } from '../adapter';

/**
 * Envelope layout: the payload is the record value (JSON), everything else
 * travels in record headers:
 *   p   pattern
 *   mid messageId - uuid generated at PRODUCE time. Offsets are per-group
 *       positions, not message identity: this header is what stays identical
 *       for every consumer group and every redelivery (the dedup key).
 *   h   user headers as JSON (byte-identical round-trip)
 *   ts  producer epoch millis
 *   c   correlationId          (requests only)
 *   r   reply topic            (requests only)
 *   to  caller timeout in ms   (requests only - lets the consumer judge
 *       per-request staleness instead of assuming a transport-wide default)
 */

export interface TransportReply {
  c: string;
  ok: boolean;
  d?: unknown;
  e?: { name: string; message: string };
}

export function buildEventHeaders(
  pattern: string,
  messageId: string,
  userHeaders: Record<string, string> | undefined,
): Record<string, string> {
  return {
    p: pattern,
    mid: messageId,
    h: JSON.stringify(userHeaders ?? {}),
    ts: String(Date.now()),
  };
}

export function buildRequestHeaders(
  pattern: string,
  messageId: string,
  userHeaders: Record<string, string> | undefined,
  correlationId: string,
  replyTopic: string,
  timeoutMs: number,
): Record<string, string> {
  return {
    ...buildEventHeaders(pattern, messageId, userHeaders),
    c: correlationId,
    r: replyTopic,
    to: String(timeoutMs),
  };
}

export function headerString(value: KafkaHeaderValue): string | undefined {
  if (value === undefined) return undefined;

  if (Array.isArray(value)) {
    return value.length ? String(value[0]) : undefined;
  }

  return String(value);
}

export function decodeHeaders(headers: Record<string, KafkaHeaderValue> | undefined): Record<string, string> {
  const decoded: Record<string, string> = {};

  for (const [key, value] of Object.entries(headers ?? {})) {
    const text = headerString(value);

    if (text !== undefined) {
      decoded[key] = text;
    }
  }

  return decoded;
}

export function buildContext(payload: KafkaEachMessagePayload, attempt: number): MessageContext {
  const raw = decodeHeaders(payload.message.headers);

  let headers: Record<string, string> = {};

  try {
    headers = JSON.parse(raw['h'] || '{}');
  } catch {
    // Malformed headers - continue with empty headers
  }

  return {
    pattern: raw['p'] ?? '',
    // Foreign producers may omit the envelope - fall back to a stable
    // position-based identity so dedup still has something to key on
    messageId: raw['mid'] ?? `${payload.topic}:${payload.partition}:${payload.message.offset}`,
    correlationId: raw['c'],
    headers,
    timestamp: Number(raw['ts']) || Number(payload.message.timestamp) || Date.now(),
    attempt,
  };
}

/**
 * Context for a record on an EXTERNAL (foreign-owned, envelope-less) topic:
 * the pattern is ALWAYS the topic name - a foreign producer's incidental `p`
 * header must not steer dispatch - and context.headers exposes ALL raw record
 * headers (traceparent, ce-*, ...) instead of the envelope's `h` JSON. The
 * messageId honors a `mid` header when present and otherwise falls back to
 * the record position, which is identical across groups and redeliveries so
 * the dedup invariant holds without an envelope. No correlationId: external
 * topics are event-only.
 */
export function buildExternalContext(payload: KafkaEachMessagePayload, attempt: number): MessageContext {
  const raw = decodeHeaders(payload.message.headers);

  return {
    pattern: payload.topic,
    messageId: raw['mid'] ?? `${payload.topic}:${payload.partition}:${payload.message.offset}`,
    headers: raw,
    timestamp: Number(payload.message.timestamp) || Date.now(),
    attempt,
  };
}

export function parsePayload(value: Buffer | null): unknown {
  if (value === null) return null;

  const text = value.toString();

  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

// --- Attempt markers (offset-commit metadata) ------------------------------

/**
 * Kafka has no per-message delivery counter; the transport persists one in
 * the offset-commit METADATA field. Committing offset X with `{"a":n}` means
 * "delivery attempt n of the record AT offset X is in progress" - a crash
 * redelivers X (the committed offset is where the next fetch starts) and the
 * metadata tells the successor how many dispatches already happened.
 */
export function encodeMarker(attempts: number): string {
  return JSON.stringify({ a: attempts });
}

export function decodeMarker(metadata: string | null | undefined): number | null {
  if (!metadata) return null;

  try {
    const parsed = JSON.parse(metadata);

    return typeof parsed?.a === 'number' && parsed.a > 0 ? parsed.a : null;
  } catch {
    return null;
  }
}
