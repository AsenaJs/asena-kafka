const TOPIC_LEGAL = /^[a-zA-Z0-9._-]+$/;
const TOPIC_MAX_LENGTH = 249;

/**
 * Kafka topic names only allow `[a-zA-Z0-9._-]` and are capped at 249 chars.
 * Message patterns become request-topic name segments, so they must be legal;
 * event patterns are matched locally against the `p` header and are exempt.
 */
export function assertTopicLegal(name: string, what: string): void {
  if (!TOPIC_LEGAL.test(name)) {
    throw new Error(`${what} "${name}" contains characters that are not Kafka-topic-legal ([a-zA-Z0-9._-])`);
  }

  if (name.length > TOPIC_MAX_LENGTH) {
    throw new Error(`${what} "${name}" exceeds Kafka's ${TOPIC_MAX_LENGTH}-character topic name limit`);
  }
}

/**
 * Topic and consumer-group naming for one transport namespace.
 *
 * Group ids are prefix-scoped: Kafka consumer groups are cluster-global
 * (unlike Redis, where groups live inside prefixed stream keys), so without
 * the prefix two environments sharing a cluster would collide on bare
 * service names.
 */
export class TopicNaming {
  private readonly prefix: string;

  public constructor(prefix: string) {
    assertTopicLegal(prefix, 'topicPrefix');
    this.prefix = prefix;
  }

  public get eventTopic(): string {
    return `${this.prefix}.evt`;
  }

  public get dlqTopic(): string {
    return `${this.prefix}.dlq`;
  }

  public get replyTopic(): string {
    return `${this.prefix}.reply`;
  }

  public requestTopic(pattern: string): string {
    return `${this.prefix}.req.${pattern}`;
  }

  public isRequestTopic(topic: string): boolean {
    return topic.startsWith(`${this.prefix}.req.`);
  }

  public patternOf(requestTopic: string): string {
    return requestTopic.slice(`${this.prefix}.req.`.length);
  }

  public groupId(serviceName: string): string {
    return `${this.prefix}.${serviceName}`;
  }

  public replyGroupId(serviceName: string, instanceId: string): string {
    return `${this.prefix}.${serviceName}.reply.${instanceId.slice(0, 8)}`;
  }
}
