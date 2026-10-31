<p width="%100" align="center">
  <img src="https://avatars.githubusercontent.com/u/179836938?s=200&v=4" width="150" align="center"/>
</p>

# @asenajs/asena-kafka

[![Version](https://img.shields.io/npm/v/@asenajs/asena-kafka.svg)](https://github.com/AsenaJs/asena-kafka)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Bun Version](https://img.shields.io/badge/Bun-1.4%2B-blueviolet)](https://bun.sh)

Kafka integration for AsenaJS: a decorator-based Kafka client and a microservice transport.

**Requirements:** Bun 1.4+, `@asenajs/asena` 0.11+, `kafkajs` 2.2.4 (peer), Apache Kafka 2.8 – 3.9.

## Installation

```bash
bun add @asenajs/asena-kafka kafkajs
```

## Quick Start

```typescript
import { Service } from '@asenajs/asena/decorators';
import { Inject } from '@asenajs/asena/decorators/ioc';
import { Kafka, AsenaKafkaService } from '@asenajs/asena-kafka';

@Kafka({
  config: { brokers: ['localhost:9092'], clientId: 'my-app' },
  name: 'AppKafka',
})
export class AppKafka extends AsenaKafkaService {}

@Service()
export class AuditService {
  @Inject(AppKafka)
  private kafka: AppKafka;

  async record(entry: { userId: string; action: string }) {
    await this.kafka.sendMessage('audit.log', [{ key: entry.userId, value: JSON.stringify(entry) }]);
  }
}
```

The service connects on `server.start()` and disconnects on `server.stop()`. Topics are never auto-created; create `audit.log` with your broker tooling or `createAdmin()`.

## Documentation

Consumers, the microservice transport, external topics and all options: [asena.sh/docs/packages/kafka](https://asena.sh/docs/packages/kafka)

## Contributing

Contributions are welcome! Please open an issue or a pull request on [GitHub](https://github.com/AsenaJs/asena-kafka).

## License

MIT — see [LICENSE](./LICENSE).
