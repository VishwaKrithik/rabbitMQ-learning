// producer.js
import amqp from 'amqplib';

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://localhost:5672';
const QUEUE_NAME = 'payments_queue';

async function sendWebhooks() {
  const connection = await amqp.connect(RABBIT_URL);
  const channel = await connection.createConfirmChannel();

  await channel.assertQueue(QUEUE_NAME, { durable: true });

  console.log('[Producer] Firing 5 checkout webhook events...');

  for (let i = 1; i <= 5; i++) {
    const paymentPayload = {
      checkout_id: `cs_test_${Math.random().toString(36).substring(2, 9)}`,
      amount_cents: Math.floor(Math.random() * 8000) + 2000,
      currency: 'usd',
      customer_email: `user_${i}@example.com`,
      created_at: new Date().toISOString(),
    };

    channel.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify(paymentPayload)),
      {
        persistent: true,
        headers: { 'x-retry-count': 0 },
      }
    );
  }

  await channel.waitForConfirms();
  console.log('[Producer] All webhooks published.');

  await channel.close();
  await connection.close();
}

sendWebhooks().catch(console.error);