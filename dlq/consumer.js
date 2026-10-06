// consumer.js
import amqp from 'amqplib'

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://localhost:5672';
const MAIN_QUEUE = 'payments_queue';
const RETRY_QUEUE = 'payments_retry_queue';
const DLQ_QUEUE = 'payments_dlq';
const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 4000;

// Chaos injection: 80% failure rate
async function mockStripeApiCall(checkoutId) {
  await new Promise((r) => setTimeout(r, 100)); // Simulated network latency
  if (Math.random() < 0.6) {
    const error = new Error('504 Gateway Timeout: Stripe backend unresponsive');
    error.status = 504;
    throw error;
  }
  return { status: 200, charge_id: `ch_${checkoutId}` };
}

async function start() {
  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createChannel();

  // 1. Declare Main Processing Queue
  await ch.assertQueue(MAIN_QUEUE, { durable: true });

  // 2. Declare Retry Queue with TTL & DLX pointing back to MAIN_QUEUE
  await ch.assertQueue(RETRY_QUEUE, {
    durable: true,
    deadLetterExchange: '',
    deadLetterRoutingKey: MAIN_QUEUE,
    messageTtl: RETRY_DELAY_MS,
  });

  // 3. Declare Permanent DLQ
  await ch.assertQueue(DLQ_QUEUE, { durable: true });

  ch.prefetch(1);
  console.log('[Consumer] Listening for checkout webhooks...');

  ch.consume(MAIN_QUEUE, async (msg) => {
    if (!msg) return;

    const payload = JSON.parse(msg.content.toString());
    const retryCount = (msg.properties.headers && msg.properties.headers['x-retry-count']) || 0;

    console.log(`\n[Process] Checkout: ${payload.checkout_id} | Attempt ${retryCount + 1}/${MAX_ATTEMPTS}`);

    try {
      await mockStripeApiCall(payload.checkout_id);
      console.log(`[Success] Payment confirmed for ${payload.checkout_id} ($${(payload.amount_cents / 100).toFixed(2)})`);
      ch.ack(msg);
    } catch (err) {
      console.error(`[Failure] ${err.message}`);

      if (retryCount + 1 < MAX_ATTEMPTS) {
        console.log(`[Retry Queue] Delaying ${payload.checkout_id} for ${RETRY_DELAY_MS / 1000}s...`);
        // Route to retry queue with incremented attempt count
        ch.sendToQueue(RETRY_QUEUE, msg.content, {
          persistent: true,
          headers: { ...msg.properties.headers, 'x-retry-count': retryCount + 1 },
        });
        ch.ack(msg); // Remove from main queue; it will re-enter after TTL
      } else {
        console.error(`[DLQ] Threshold exceeded (${MAX_ATTEMPTS} attempts). Moving ${payload.checkout_id} to ${DLQ_QUEUE}`);
        ch.sendToQueue(DLQ_QUEUE, msg.content, {
          persistent: true,
          headers: { ...msg.properties.headers, 'x-retry-count': retryCount + 1, 'x-failure-reason': err.message },
        });
        ch.ack(msg);
      }
    }
  });
}

start().catch(console.error);