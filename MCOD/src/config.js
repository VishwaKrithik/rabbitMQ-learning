export const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
export const EXCHANGE_NAME = 'store.events';
export const EXCHANGE_TYPE = 'topic';