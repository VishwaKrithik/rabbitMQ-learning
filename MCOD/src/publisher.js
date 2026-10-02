import amqp from 'amqplib';
import { RABBITMQ_URL, EXCHANGE_NAME, EXCHANGE_TYPE } from './config.js';

const events = [
  {
    routingKey: 'order.us.created',
    payload: { orderId: 'US-9011', amount: 120.50, currency: 'USD', status: 'PLACED' }
  },
  {
    routingKey: 'order.eu.created',
    payload: { orderId: 'EU-4022', amount: 89.00, currency: 'EUR', status: 'PLACED' }
  },
  {
    routingKey: 'order.us.cancelled',
    payload: { orderId: 'US-9011', reason: 'Customer requested refund', status: 'CANCELLED' }
  }
];

async function publishEvents() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  // Ensure the exchange exists before publishing
  await channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, { durable: true });

  console.log(`[Publisher] Connected. Emitting events to exchange: "${EXCHANGE_NAME}"\n`);

  for (const event of events) {
    const messageBuffer = Buffer.from(JSON.stringify(event.payload));

    channel.publish(EXCHANGE_NAME, event.routingKey, messageBuffer, {
      persistent: true,
      contentType: 'application/json'
    });

    console.log(`[Sent] Routing Key: "${event.routingKey}" | Payload:`, event.payload);
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  await channel.close();
  await connection.close();
  process.exit(0);
}

publishEvents().catch(console.error);