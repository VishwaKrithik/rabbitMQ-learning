// producer.js
import amqp from 'amqplib'

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://localhost:5672';
const QUEUE_NAME = 'order_quorum_queue';
const TOTAL_MESSAGES = 100_000;

async function run() {
  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createConfirmChannel();

  // Listen to connection/channel-level blocks from broker alarms
  conn.on('blocked', (reason) => {
    console.warn(`\n[CRITICAL] Broker ALARM active! Connection blocked: ${reason}`);
  });
  conn.on('unblocked', () => {
    console.log('\n[INFO] Broker ALARM cleared. Connection unblocked.');
  });

  // Declare modern Quorum Queue
  await ch.assertQueue(QUEUE_NAME, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum', // Activates Raft consensus engine
    },
  });

  console.log(`[Producer] Publishing ${TOTAL_MESSAGES.toLocaleString()} orders to "${QUEUE_NAME}"...`);

  let published = 0;

  for (let i = 1; i <= TOTAL_MESSAGES; i++) {
    const order = {
      order_id: `ord_${i}`,
      sku: `sku_${i % 250}`,
      price: (Math.random() * 100).toFixed(2),
      ts: Date.now(),
    };

    const canContinue = ch.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify(order)),
      {
        persistent: true,
        contentType: 'application/json',
      }
    );

    published++;

    if (published % 5000 === 0) {
      process.stdout.write(`\r[Producer] Buffered: ${published.toLocaleString()} messages...`);
    }

    // Handle standard client-side stream buffer backpressure
    if (!canContinue) {
      await new Promise((resolve) => ch.once('drain', resolve));
    }
  }

  console.log('\n[Producer] Flushing pending writes to broker disk log...');
  await ch.waitForConfirms();
  console.log(`[Producer] Confirmed all ${TOTAL_MESSAGES.toLocaleString()} messages persisted.`);

  await ch.close();
  await conn.close();
}

run().catch(console.error);