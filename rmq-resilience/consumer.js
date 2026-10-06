// consumer.js
import amqp from 'amqplib';

const QUEUE_NAME = 'flash_sale_vouchers';
const RABBIT_URL = process.env.RABBIT_URL || 'amqp://localhost:5672';

// Idempotency store (in-memory Set; substitute with Redis in production)
const processedVoucherIds = new Set();

async function startConsumer() {
  try {
    const connection = await amqp.connect(RABBIT_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE_NAME, {
      durable: true,
    });

    // Process one message at a time to prevent unhandled un-acked pileups
    await channel.prefetch(1);

    console.log('[Consumer] Waiting for vouchers. Press CTRL+C to exit.');

    channel.consume(
      QUEUE_NAME,
      async (msg) => {
        if (!msg) return;

        try {
          const content = JSON.parse(msg.content.toString());
          const { voucher_id } = content;

          // Idempotency Guard
          if (processedVoucherIds.has(voucher_id)) {
            console.warn(`[Consumer] Duplicate skipped: ${voucher_id}`);
            channel.ack(msg); // Acknowledge to clear redundant delivery from queue
            return;
          }

          // Simulate business processing (e.g., database commit)
          processedVoucherIds.add(voucher_id);
          console.log(
            `[Consumer] Processed ${voucher_id} (Total Unique: ${processedVoucherIds.size})`
          );

          // Acknowledge after safe completion
          channel.ack(msg);
        } catch (err) {
          console.error('[Consumer] Processing failed:', err.message);
          // nack without requeue if payload is malformed; requeue if transient failure
          channel.nack(msg, false, false);
        }
      },
      {
        noAck: false, // Strict manual acknowledgment
      }
    );
  } catch (error) {
    console.error('[Consumer] Fatal error:', error.message);
  }
}

startConsumer().catch(console.error);