# AMQP Client

## About

This module aims to ease the communication with a broker using the AMQP protocol, it provides some low-level tools.

## Getting started

## Prerequisites

This module requires Node 14 and a connection to an AMQP broker

## Installation

```javascript
import Client from '@prismamedia/amqp-client';
// or : Client = require('@prismamedia/amqp-client');
```

## Configuration

```javascript
const client = new Client('amqp://rabbitmq/');
```

## Usage

```javascript
const payload = { ... };

try {
  await client.publish('', 'queue_name', payload);
} catch (err) {
  // Handle error
}
```

```javascript
try {
  const consumerId = await client.consume(
    'queue_name',
    (message, payload, ack) => {
      // Do what you want with the full AMQP message or with the Object payload

      ack();
    },
  );

  // [...]

  await client.stopConsumer(consumerId, true);
} catch (err) {
  // Handle error
}
```
