import amqp from 'amqplib';
import { RABBITMQ_URL, EXCHANGE_NAME, EXCHANGE_TYPE } from './config.js';

const BINDING_KEY = 'order.us.*';
const QUEUE_NAME = 'us_warehouse_queue';

async function start() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, { durable: true });
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, BINDING_KEY);

  console.log(`[US Warehouse] Listening on queue "${QUEUE_NAME}" with pattern: "${BINDING_KEY}"...`);

  channel.consume(QUEUE_NAME, (msg) => {
    if (!msg) return;
    const data = JSON.parse(msg.content.toString());
    const routingKey = msg.fields.routingKey;

    console.log(`\n[US Warehouse Action] Key: "${routingKey}"`);
    console.log(`Order ${data.orderId} processing: ${data.status}`);
    channel.ack(msg);
  });
}

start().catch(console.error);