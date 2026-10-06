// producer.js
import amqp from 'amqplib';

const QUEUE_NAME = 'flash_sale_vouchers';
const RABBIT_URL = process.env.RABBIT_URL || 'amqp://localhost:5672';

async function publishVouchers(total = 50) {
  let connection;
  let channel;

  try {
    connection = await amqp.connect(RABBIT_URL);
    // 1. Initialize confirm channel instead of regular channel
    channel = await connection.createConfirmChannel();

    // 2. Declare queue as durable
    await channel.assertQueue(QUEUE_NAME, {
      durable: true,
    });

    console.log(`[Producer] Connected. Publishing ${total} durable vouchers...`);

    for (let i = 1; i <= total; i++) {
      const voucher = {
        voucher_id: `vchr_${String(i).padStart(4, '0')}`,
        discount: '25%',
        timestamp: new Date().toISOString(),
      };

      const payload = Buffer.from(JSON.stringify(voucher));

      // 3. Publish with persistent: true and await broker confirmation
      await new Promise((resolve, reject) => {
        channel.sendToQueue(
          QUEUE_NAME,
          payload,
          {
            persistent: true, // deliveryMode: 2
            contentType: 'application/json',
          },
          (err, ok) => {
            if (err) {
              return reject(err);
            }
            resolve(ok);
          }
        );
      });

      console.log(`[Producer] Persisted & Confirmed: ${voucher.voucher_id}`);

      // Small delay to make it easy to kill the container mid-flight if testing manually
      await new Promise((res) => setTimeout(res, 80));
    }

    console.log(`[Producer] All ${total} vouchers safely acknowledged by broker.`);
  } catch (error) {
    console.error('[Producer] Error during publishing:', error.message);
  } finally {
    if (channel) await channel.close();
    if (connection) await connection.close();
  }
}

publishVouchers(50).catch(console.error);