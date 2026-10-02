import amqp from 'amqplib';
import dotenv from 'dotenv';

dotenv.config();

let channel = null;

export async function getChannel() {
    if (channel) return channel;

    const connection = await amqp.connect(process.env.RABBITMQ_URL)
    channel = await connection.createChannel();
    await channel.assertQueue(process.env.QUEUE_NAME, {durable: true});
    return channel;
}