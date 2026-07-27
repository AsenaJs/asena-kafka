import { afterEach, describe, expect, it } from 'bun:test';
import { AsenaServerFactory } from '@asenajs/asena';
import { Config, MessageController } from '@asenajs/asena/decorators';
import { EventPattern, MessagePattern } from '@asenajs/asena/microservice';
import { ICoreServiceNames } from '@asenajs/asena/ioc/types';
import { KafkaMicroserviceTransport } from '../lib/microservice';
import type { KafkaMicroserviceOptions } from '../lib/types';
import { BROKERS, quietLogger, sleep, sweepNamespace, uniquePrefix, waitFor } from './util';

const FAST: Omit<KafkaMicroserviceOptions, 'serviceName'> = {
  sessionTimeout: 3000,
  heartbeatInterval: 800,
  retryBackoffMs: 300,
  healthCheckIntervalMs: 1000,
  drainTimeout: 500,
};

/**
 * End-to-end scenario over real Kafka: a HEADLESS Asena service (no HTTP
 * adapter) driven purely by microservice messages, talked to by a raw client
 * transport - exercising the full chain factory → config →
 * PrepareMicroserviceService → Ulak → KafkaMicroserviceTransport.
 */
describe('Headless E2E over Kafka', () => {
  let server: any;
  let client: KafkaMicroserviceTransport | undefined;
  const topicPrefix = uniquePrefix('e2e');

  afterEach(async () => {
    await client?.destroy({ drainTimeout: 500 }).catch(() => {});
    client = undefined;
    await server?.stop();
    server = undefined;
    await sweepNamespace(topicPrefix);
  }, 30000);

  it('should run a message-driven headless service end-to-end', async () => {
    const paymentsReceived: any[] = [];
    const headerEcho: Record<string, string>[] = [];

    @Config()
    class HeadlessConfig {
      public transport() {
        return {
          microservice: new KafkaMicroserviceTransport(
            { brokers: BROKERS },
            { serviceName: 'order-service', topicPrefix, ...FAST },
          ),
        };
      }
    }

    @MessageController('order')
    class OrderHandler {
      @MessagePattern('create')
      public async create(data: any) {
        return { id: 101, ...data };
      }

      @MessagePattern('reject')
      public async reject() {
        throw new Error('NOT_FOUND: no such order');
      }

      @MessagePattern('echoHeaders')
      public async echoHeaders(_data: any, context: any) {
        return context.headers;
      }

      @EventPattern({ pattern: 'payment.*', prefix: false })
      public async onPayment(data: any, context: any) {
        paymentsReceived.push(data);
        headerEcho.push(context.headers);
      }
    }

    // 10000-31999: above the well-known range and below the kernel's ephemeral floor
    // (net.ipv4.ip_local_port_range, 32768-60999). Drawing a *server* port from the
    // ephemeral range collides with the outbound sockets the suite itself holds open -
    // including their 60s TIME_WAIT - and Bun.serve then fails with EADDRINUSE.
    const healthPort = 10000 + Math.floor(Math.random() * 22000);

    server = await AsenaServerFactory.create({
      headless: true,
      logger: quietLogger,
      components: [HeadlessConfig, OrderHandler],
      health: { port: healthPort },
    });

    await server.start();

    // No HTTP adapter was registered - the service is truly headless
    expect(server.coreContainer.container.has(ICoreServiceNames.ASENA_ADAPTER)).toBe(false);

    // Health endpoint reports the transport as connected
    const health = await fetch(`http://localhost:${healthPort}/healthz`);
    const healthBody: any = await health.json();

    expect(health.status).toBe(200);
    expect(healthBody.transports.default).toBe('connected');

    // A separate "service" (raw client transport, zero handlers = client-only
    // mode: listen() starts no consumer) does RPC against the headless app
    client = new KafkaMicroserviceTransport(
      { brokers: BROKERS },
      { serviceName: 'checkout-service', topicPrefix, ...FAST },
    );
    await client.init();
    await client.listen(); // no handlers - must be a no-op

    const reply = await client.send<{ id: number; total: number }>('order.create', { total: 25 }, { timeout: 15000 });

    expect(reply).toEqual({ id: 101, total: 25 });

    // RPC errors surface faithfully and are final
    await expect(client.send('order.reject', {}, { timeout: 15000 })).rejects.toThrow('NOT_FOUND');

    // Headers propagate byte-identically on send...
    const echoed = await client.send<Record<string, string>>(
      'order.echoHeaders',
      {},
      { timeout: 15000, headers: { 'x-request-id': 'req-42', 'x-tenant': 'acme' } },
    );

    expect(echoed).toEqual({ 'x-request-id': 'req-42', 'x-tenant': 'acme' });

    // ...and choreography: emits an event the headless app reacts to via
    // wildcard, with emit headers intact
    await client.emit('payment.completed', { orderId: 101 }, { headers: { 'x-trace': 'abc' } });

    await waitFor(() => paymentsReceived.length > 0, 15000, 'payment event delivery');

    expect(paymentsReceived).toEqual([{ orderId: 101 }]);
    expect(headerEcho[0]).toEqual({ 'x-trace': 'abc' });

    // Unknown pattern: the topic exists but nobody consumes it - the caller
    // times out (capability 'timeout')
    const unknownStart = Date.now();

    await expect(client.send('order.doesNotExist', {}, { timeout: 1500 })).rejects.toThrow('timed out');
    expect(Date.now() - unknownStart).toBeLessThan(5000);

    // Settle: nothing arrives twice
    await sleep(500);
    expect(paymentsReceived.length).toBe(1);
  }, 90000);
});
