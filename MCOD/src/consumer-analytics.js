import amqp from 'amqplib';
import { RABBITMQ_URL, EXCHANGE_NAME, EXCHANGE_TYPE } from './config.js';

const BINDING_KEY = 'order.*.created';
const QUEUE_NAME = 'global_analytics_queue';

async function start() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, { durable: true });
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, BINDING_KEY);

  console.log(`[Analytics] Listening on queue "${QUEUE_NAME}" with pattern: "${BINDING_KEY}"...`);

  channel.consume(QUEUE_NAME, (msg) => {
    if (!msg) return;
    const data = JSON.parse(msg.content.toString());
    const routingKey = msg.fields.routingKey;

    console.log(`\n[Analytics Metric] Key: "${routingKey}"`);
    console.log(`Revenue recorded: +${data.amount} ${data.currency} (Order: ${data.orderId})`);
    channel.ack(msg);
  });
}

start().catch(console.error);