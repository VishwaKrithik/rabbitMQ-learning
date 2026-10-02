import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import PDFDocument from 'pdfkit';
import { getChannel } from './rabbitmq.js';
import amqp from 'amqplib';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const invoicesDir = path.resolve(__dirname, '../invoices');

if (!fs.existsSync(invoicesDir)) {
  fs.mkdirSync(invoicesDir, { recursive: true });
}

function generatePDF(data) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ margin: 50 });
    const filePath = path.join(invoicesDir, `invoice-${data.invoiceId}.pdf`);
    const stream = fs.createWriteStream(filePath);

    doc.pipe(stream);

    // Header
    doc.fontSize(20).text('INVOICE', { align: 'right' });
    doc.moveDown();
    doc.fontSize(12).text(`Invoice ID: ${data.invoiceId}`);
    doc.text(`Customer: ${data.customerName}`);
    doc.text(`Date: ${new Date().toLocaleDateString()}`);
    doc.moveDown();

    // Table divider
    doc.moveTo(50, doc.y).lineTo(550, doc.y).stroke();
    doc.moveDown(0.5);

    // Line items
    doc.font('Helvetica-Bold').text('Description', 50, doc.y, { continued: true });
    doc.text('Amount', { align: 'right' });
    doc.font('Helvetica');

    data.items.forEach(item => {
      doc.text(item.description, 50, doc.y, { continued: true });
      doc.text(`$${Number(item.price).toFixed(2)}`, { align: 'right' });
    });

    doc.moveDown();
    doc.moveTo(50, doc.y).lineTo(550, doc.y).stroke();
    doc.moveDown(0.5);

    // Total
    doc.fontSize(14).font('Helvetica-Bold');
    doc.text(`Total: $${Number(data.totalAmount).toFixed(2)}`, { align: 'right' });

    doc.end();

    stream.on('finish', () => resolve(filePath));
    stream.on('error', reject);
  });
}

async function startWorker() {
  const channel = await getChannel();
  
  channel.prefetch(1);

  console.log(`Worker active. Awaiting jobs in "${process.env.QUEUE_NAME}"...`);

  channel.consume(process.env.QUEUE_NAME, async (msg) => {
    if (!msg) return;

    const data = JSON.parse(msg.content.toString());
    console.log(`Processing invoice: ${data.invoiceId}`);

    try {
      const generatedPath = await generatePDF(data);
      console.log(`Successfully generated: ${generatedPath}`);

      // Acknowledge completion so message is removed from the queue
      channel.ack(msg);
    } catch (err) {
      console.error(`Failed to generate invoice ${data.invoiceId}:`, err);

      // Negative acknowledgment: requeue = false sends to DLQ if configured, or discards poisoned message
      channel.nack(msg, false, false);
    }
  });
}

startWorker().catch(console.error);