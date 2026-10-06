// replayDlq.js
import amqp from 'amqplib'
import readline from 'readline'

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://localhost:5672';
const DLQ = 'payments_dlq';
const TARGET_QUEUE = 'payments_queue';

async function replay() {
  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createChannel();
  const q = await ch.checkQueue(DLQ);

  console.log(`[DLQ Inspector] Found ${q.messageCount} messages parked in "${DLQ}".`);
  if (q.messageCount === 0) return await conn.close();

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.question('Press [ENTER] to replay all failed transactions back to the main queue (Ctrl+C to abort)...', async () => {
    let replayed = 0;
    while (true) {
      const msg = await ch.get(DLQ, { noAck: false });
      if (!msg) break;

      const content = JSON.parse(msg.content.toString());
      console.log(`-> Replaying: ${content.checkout_id} (Reason: ${msg.properties.headers['x-failure-reason']})`);

      // Reset retry count to 0 and push back to main queue
      ch.sendToQueue(TARGET_QUEUE, msg.content, {
        persistent: true,
        headers: { ...msg.properties.headers, 'x-retry-count': 0, 'x-replayed-at': new Date().toISOString() },
      });
      ch.ack(msg);
      replayed++;
    }
    console.log(`[Done] Successfully replayed ${replayed} messages.`);
    rl.close();
    await conn.close();
  });
}

replay().catch(console.error);