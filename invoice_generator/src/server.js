import dotenv from 'dotenv';
import { getChannel } from './rabbitmq.js';
import express from "express";

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  return res.status(200).json({"message": "Server running"});
})

app.post('/api/invoices', async (req, res) => {
  const { invoiceId, customerName, items, totalAmount } = req.body;

  if (!invoiceId || !customerName || !items || totalAmount === undefined) {
    return res.status(400).json({ error: 'Missing required invoice payload fields.' });
  }

  try {
    const channel = await getChannel();
    const payload = Buffer.from(JSON.stringify({ invoiceId, customerName, items, totalAmount }));

    // persistent: true ensures messages are written to disk
    channel.sendToQueue(process.env.QUEUE_NAME, payload, { persistent: true });

    return res.status(202).json({
      status: 'Queued',
      message: `Invoice job ${invoiceId} has been enqueued for generation.`
    });
  } catch (error) {
    console.error('Queue dispatch error:', error);
    return res.status(500).json({ error: 'Failed to enqueue invoice task.' });
  }
});


const PORT = process.env.PORT || 3000;
app.listen(3000, () => {
    console.log("Listening on port 3000");
})