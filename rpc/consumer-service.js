const amqp = require('amqplib');
const axios = require('axios');

const RABBIT_URL = 'amqp://localhost';
// const REQUEST_QUEUE = 'sales_order';
// const ERP_ENDPOINT = 'http://127.0.0.1:3000/salesorder';
const QUEUE_HANDLERS = {
  sales_order: {
    endpoint: 'http://127.0.0.1:3000/salesorder'
  },
  check_inventory: {
    endpoint: 'http://127.0.0.1:3000/checkinventory'
  },
  check_order: {
    endpoint: 'http://127.0.0.1:3000/checkorder'
  }
};



async function consumeQueue(channel, queueName, handler) {
  await channel.assertQueue(queueName, { durable: false });

  channel.consume(queueName, async (msg) => {
    if (!msg) return;
    console.log("queuename=", queueName);
    let payload;
    try {
      payload = JSON.parse(msg.content.toString());
    } catch (e) {
      channel.ack(msg);
      return;
    }

    const { correlationId, replyTo } = msg.properties;

    let response;
    try {
      const erpResponse = await axios.post(handler.endpoint, payload);
      response = { success: true, data: erpResponse.data };

    } catch (err) {
      response = {
        success: false,
        error: err.response?.data || err.message
      };
    }

    channel.sendToQueue(
      replyTo,
      Buffer.from(JSON.stringify(response)),
      { correlationId }
    );

    channel.ack(msg);
  });

  console.log(`🟢 Listening on ${queueName}`);
}

async function start() {
  const connection = await amqp.connect(RABBIT_URL);
  const channel = await connection.createChannel();

  channel.prefetch(1);

  for (const [queueName, handler] of Object.entries(QUEUE_HANDLERS)) {
    await consumeQueue(channel, queueName, handler);
  }
}

start().catch(console.error);



// async function start() {
//   const connection = await amqp.connect(RABBIT_URL);
//   const channel = await connection.createChannel();

//   await channel.assertQueue(REQUEST_QUEUE, {
//     durable: false
//   });

//   // Important: process one message at a time
//   channel.prefetch(1);

//   console.log('🟢 Waiting for RPC requests...');

//   channel.consume(REQUEST_QUEUE, async (msg) => {
//     if (!msg) return;

//     const payload = JSON.parse(msg.content.toString());
//     const correlationId = msg.properties.correlationId;
//     const replyTo = msg.properties.replyTo;

//     console.log('📩 Received order:', payload);

//     let response;

//     try {
//       // 🔹 Call ERP
//       const erpResponse = await axios.post(ERP_ENDPOINT, payload);

//       response = {
//         success: true,
//         data: erpResponse.data
//       };
//     } catch (err) {
//       response = {
//         success: false,
//         error: err.response?.data || err.message
//       };
//     }

//     // 🔹 Send response back to RPC client
//     channel.sendToQueue(
//       replyTo,
//       Buffer.from(JSON.stringify(response)),
//       {
//         correlationId
//       }
//     );

//     // 🔹 Acknowledge message
//     channel.ack(msg);
//   });
// }


