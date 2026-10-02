import amqp from 'amqplib';
import { RABBITMQ_URL, QUEUE_NAME } from './config.js';

// Parse arguments robustly (handles both --flag=val and --flag val)
function parseArgs() {
  const args = process.argv.slice(2);
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      const parts = args[i].replace(/^--/, '').split('=');
      const key = parts[0];
      const val = parts.length > 1 ? parts[1] : args[i + 1];
      result[key] = val;
    }
  }
  return result;
}

const parsed = parseArgs();

const WORKER_ID = parsed.id || 'Worker-Default';
const PROCESS_DELAY_MS = Number(parsed.speed ?? 200);
const PREFETCH_COUNT = Number(parsed.prefetch ?? 1);

let jobsHandled = 0;

async function start() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertQueue(QUEUE_NAME, { durable: true });

  // Apply prefetch
  if (PREFETCH_COUNT > 0) {
    channel.prefetch(PREFETCH_COUNT);
    console.log(`[${WORKER_ID}] Online -> Delay: ${PROCESS_DELAY_MS}ms | Prefetch: ${PREFETCH_COUNT}`);
  } else {
    console.log(`[${WORKER_ID}] Online -> Delay: ${PROCESS_DELAY_MS}ms | Prefetch: UNBOUNDED (0)`);
  }

  channel.consume(
    QUEUE_NAME,
    async (msg) => {
      if (!msg) return;

      const job = JSON.parse(msg.content.toString());
      const startTime = new Date().toISOString();
      console.log(`[${WORKER_ID}] [START] Job #${job.jobId} at ${startTime}`);

      try {
        // Explicit sleep
        await new Promise((res) => setTimeout(res, PROCESS_DELAY_MS));

        jobsHandled++;
        const finishTime = new Date().toISOString();
        console.log(`[${WORKER_ID}] [DONE]  Job #${job.jobId} at ${finishTime} (Total: ${jobsHandled})`);

        channel.ack(msg);
      } catch (err) {
        console.error(`[${WORKER_ID}] [ERR]   Job #${job.jobId}:`, err.message);
        channel.nack(msg, false, false);
      }
    },
    { noAck: false }
  );
}

start().catch(console.error);