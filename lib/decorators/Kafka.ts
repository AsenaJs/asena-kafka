import 'reflect-metadata';
import type { KafkaDecoratorOptions } from '../types';
import { AsenaKafkaService } from '../AsenaKafkaService';
import { Service } from '@asenajs/asena/decorators';
import { defineMetadata, getMetadata, getMetadataKeys } from 'reflect-metadata/no-conflict';

export function Kafka(options: KafkaDecoratorOptions) {
  return function <T extends new (...args: any[]) => any>(target: T) {
    @Service(options.name || target.name)
    class KafkaServiceClass extends AsenaKafkaService {
      public constructor() {
        super();

        if (!options.logger) {
          options.logger = console;
        }

        this.setKafkaOptions(options);

        if (options.client) {
          this.setKafkaClient(options.client);
        }
      }
    }

    // Copy prototype methods from target
    Object.getOwnPropertyNames(target.prototype).forEach((name) => {
      if (name !== 'constructor') {
        const descriptor = Object.getOwnPropertyDescriptor(target.prototype, name);

        if (descriptor) {
          Object.defineProperty(KafkaServiceClass.prototype, name, descriptor);
        }
      }
    });

    // Copy static methods and properties
    Object.getOwnPropertyNames(target).forEach((name) => {
      if (name !== 'prototype' && name !== 'name' && name !== 'length') {
        const descriptor = Object.getOwnPropertyDescriptor(target, name);

        if (descriptor) {
          Object.defineProperty(KafkaServiceClass, name, descriptor);
        }
      }
    });

    // Copy metadata
    const metadata = getMetadataKeys(target);

    metadata.forEach((key) => {
      const value = getMetadata(key, target);

      defineMetadata(key, value, KafkaServiceClass);
    });

    // Override class name to match original target
    Object.defineProperty(KafkaServiceClass, 'name', {
      value: target.name,
      writable: false,
      configurable: true,
    });

    return KafkaServiceClass as any;
  };
}
