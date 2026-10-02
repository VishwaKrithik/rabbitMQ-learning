import amqp from 'amqplib';
import { RABBITMQ_URL, QUEUE_NAME } from './config.js';

const SIZES = ['small', 'medium', 'huge'];

async function publishJobs() {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue(QUEUE_NAME, { durable: true });
    await channel.purgeQueue(QUEUE_NAME);

    for (let i = 1; i <= 20; i++) {
        const job = {
            jobId: i,
            imageName: `asset-${i}.png`,
            size: SIZES[i % SIZES.length]
        };
        channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(job)), {
            persistent: true
        });
    }
    console.log('[Publisher] All 20 jobs dispatched successfully.');
    await channel.close();
    await connection.close();
    process.exit(0);
}

publishJobs().catch(console.error);