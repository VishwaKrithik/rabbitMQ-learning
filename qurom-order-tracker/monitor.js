// monitor.js
const QUEUE_NAME = 'order_quorum_queue';
const MGMT_URL = process.env.RABBIT_MGMT_URL || 'http://localhost:15672';
const USER = 'guest';
const PASS = 'guest';

const authHeader = `Basic ${Buffer.from(`${USER}:${PASS}`).toString('base64')}`;

async function pollMetrics() {
  try {
    // 1. Fetch Queue Telemetry
    const qRes = await fetch(`${MGMT_URL}/api/queues/%2F/${QUEUE_NAME}`, {
      headers: { Authorization: authHeader },
    });

    // 2. Fetch Node Health & Alarms
    const nodeRes = await fetch(`${MGMT_URL}/api/nodes`, {
      headers: { Authorization: authHeader },
    });

    if (qRes.ok && nodeRes.ok) {
      const q = await qRes.json();
      const nodes = await nodeRes.json();
      const node = nodes[0]; // Primary local node

      const ramUsageMb = (node.mem_used / 1024 / 1024).toFixed(1);
      const ramLimitMb = (node.mem_limit / 1024 / 1024).toFixed(1);
      const isMemAlarm = node.mem_alarm;

      console.clear();
      console.log('====================================================');
      console.log(` RABBITMQ QUORUM TELEMETRY & HEALTH MONITOR`);
      console.log('====================================================');
      console.log(`Target Queue         : ${q.name} (Type: ${q.type || 'quorum'})`);
      console.log(`Ready Messages       : ${q.messages_ready?.toLocaleString() ?? 0}`);
      console.log(`Unacknowledged (In-Flight): ${q.messages_unacknowledged ?? 0}`);
      console.log(`Total Queue Depth    : ${q.messages?.toLocaleString() ?? 0}`);
      console.log(`Active Consumers     : ${q.consumers ?? 0}`);
      console.log('----------------------------------------------------');
      console.log(`Broker RAM Used      : ${ramUsageMb} MB / ${ramLimitMb} MB`);
      console.log(`Memory Alarm Triggered: ${isMemAlarm ? 'YES (PUBLISHERS BLOCKED)' : 'NO'}`);
      console.log('====================================================');
    } else if (qRes.status === 404) {
      console.log(`[Monitor] Waiting for queue "${QUEUE_NAME}" to be declared...`);
    }
  } catch (err) {
    console.error('[Monitor] Connection error:', err.message);
  }
}

setInterval(pollMetrics, 1000);
pollMetrics();